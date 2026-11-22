using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Operators.Windows.Triggers;
using Cortex.Streams.Watermarks;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Cortex.Streams.Operators.Windows
{
    /// <summary>
    /// An advanced session window operator with support for custom triggers and state modes.
    /// </summary>
    /// <remarks>
    /// Downstream of <c>WithWatermarks</c> sessions work in event time, as in <see cref="SessionWindowOperator{TInput, TKey}"/>:
    /// overlapping event intervals merge, so out-of-order events extend or bridge sessions. Triggers receive the watermark
    /// wherever they would receive the processing time and run on every element. A session fires its final result when
    /// its trigger purges or the watermark passes its last activity plus the gap; an event that joins it within the
    /// allowed lateness fires a <see cref="WindowEmissionType.Late"/> result, and later events are dropped and reported.
    /// <see cref="WindowConfiguration{TInput}.AllowedLateness"/> and <see cref="WindowConfiguration{TInput}.OnLateEvent"/>
    /// take precedence over the stream's watermark strategy when set.
    /// </remarks>
    /// <typeparam name="TInput">The type of the input items.</typeparam>
    /// <typeparam name="TKey">The type of the key used to partition sessions.</typeparam>
    public class AdvancedSessionWindowOperator<TInput, TKey> : IOperator, IStatefulOperator, ITelemetryEnabled, IWatermarkAware, IDisposable
    {
        private static readonly string OperatorName = $"AdvancedSessionWindowOperator<{typeof(TInput).Name}>";

        private readonly Func<TInput, TKey> _keySelector;
        private readonly Func<TInput, DateTime> _timestampSelector;
        private readonly TimeSpan _inactivityGap;
        private readonly IDataStore<string, AdvancedSessionState<TInput>> _stateStore;
        private readonly WindowConfiguration<TInput> _config;
        private readonly object _lock = new object();
        private IOperator _nextOperator;
        private Timer _sessionTimer;
        private bool _disposed;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Event time, guarded by _lock: null until the stream enables it
        private EventTimeContext _eventTime;
        private long _latenessTicks;
        private long _watermarkTicks;
        private readonly Dictionary<string, List<string>> _sessionsByKey = new Dictionary<string, List<string>>();
        private readonly HashSet<string> _firedSessions = new HashSet<string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="AdvancedSessionWindowOperator{TInput, TKey}"/> class.
        /// </summary>
        /// <param name="keySelector">A function to extract the key from each input item.</param>
        /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
        /// <param name="inactivityGap">The duration of inactivity after which a session is closed.</param>
        /// <param name="stateStore">The state store to use for storing session data.</param>
        /// <param name="config">The window configuration.</param>
        public AdvancedSessionWindowOperator(
            Func<TInput, TKey> keySelector,
            Func<TInput, DateTime> timestampSelector,
            TimeSpan inactivityGap,
            IDataStore<string, AdvancedSessionState<TInput>> stateStore,
            WindowConfiguration<TInput> config = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _timestampSelector = timestampSelector ?? throw new ArgumentNullException(nameof(timestampSelector));
            _inactivityGap = inactivityGap;
            _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
            _config = config ?? new WindowConfiguration<TInput>();

            // Start session evaluation timer
            _sessionTimer = new Timer(EvaluateSessions, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"advanced_session_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by AdvancedSessionWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"advanced_session_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for AdvancedSessionWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"AdvancedSessionWindowOperator_{typeof(TInput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _recordProcessingTime = null;
            }

            // Propagate telemetry
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public void Process(object input)
        {
            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("AdvancedSessionWindowOperator.Process"))
                {
                    try
                    {
                        ProcessInternal(input);
                        span.SetAttribute("status", "success");
                    }
                    catch (Exception ex)
                    {
                        span.SetAttribute("status", "error");
                        span.SetAttribute("exception", ex.ToString());
                        throw;
                    }
                    finally
                    {
                        stopwatch.Stop();
                        _recordProcessingTime(stopwatch.Elapsed.TotalMilliseconds);
                        _incrementProcessedCounter();
                    }
                }
            }
            else
            {
                ProcessInternal(input);
            }
        }

        private void ProcessInternal(object input)
        {
            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);
            var timestamp = _timestampSelector(typedInput);

            if (Volatile.Read(ref _eventTime) != null)
            {
                ProcessEventTime(typedInput, key.ToString(), timestamp);
                return;
            }

            var sessionKey = GetSessionKey(key);

            lock (_lock)
            {
                var session = _stateStore.Get(sessionKey);

                if (session == null)
                {
                    // Create new session
                    session = new AdvancedSessionState<TInput>
                    {
                        Key = key.ToString(),
                        StartTime = timestamp,
                        LastActivityTime = timestamp,
                        Items = new List<TInput> { typedInput },
                        TriggerContext = new TriggerContext<TInput>(sessionKey, () => _stateStore.Get(sessionKey)?.Items?.Count ?? 0),
                        EmissionSequence = 0,
                        LastEmittedItems = new List<TInput>()
                    };
                    _stateStore.Put(sessionKey, session);
                }
                else
                {
                    // Check if the event is within the inactivity gap
                    var timeSinceLastActivity = timestamp - session.LastActivityTime;

                    if (timeSinceLastActivity > _inactivityGap)
                    {
                        // Close the current session and emit it
                        EmitSession(sessionKey, session, isFinal: true);

                        // Start a new session
                        session = new AdvancedSessionState<TInput>
                        {
                            Key = key.ToString(),
                            StartTime = timestamp,
                            LastActivityTime = timestamp,
                            Items = new List<TInput> { typedInput },
                            TriggerContext = new TriggerContext<TInput>(sessionKey, () => _stateStore.Get(sessionKey)?.Items?.Count ?? 0),
                            EmissionSequence = 0,
                            LastEmittedItems = new List<TInput>()
                        };
                        _stateStore.Put(sessionKey, session);
                    }
                    else
                    {
                        // Extend the current session
                        session.Items.Add(typedInput);
                        session.LastActivityTime = timestamp;
                        _stateStore.Put(sessionKey, session);

                        // Evaluate trigger on element
                        var windowEnd = session.LastActivityTime + _inactivityGap;
                        var triggerResult = _config.Trigger.OnElement(typedInput, timestamp, session.StartTime, windowEnd, session.TriggerContext);
                        
                        if (triggerResult == TriggerResult.Fire)
                        {
                            EmitSession(sessionKey, session, isFinal: false);
                        }
                        else if (triggerResult == TriggerResult.FireAndPurge)
                        {
                            EmitSession(sessionKey, session, isFinal: true);
                            _stateStore.Remove(sessionKey);
                        }
                    }
                }
            }
        }

        private string GetSessionKey(TKey key)
        {
            return $"session_{key}";
        }

        private void ProcessEventTime(TInput item, string key, DateTime timestamp)
        {
            bool late;
            long watermarkTicks;

            lock (_lock)
            {
                watermarkTicks = _watermarkTicks;
                late = timestamp.Ticks + _inactivityGap.Ticks + _latenessTicks < watermarkTicks;

                if (!late)
                {
                    AddToSessions(item, key, timestamp, watermarkTicks);
                }
            }

            if (late)
            {
                if (_config.OnLateEvent != null)
                {
                    _eventTime.InvokeLateCallback(() => _config.OnLateEvent(item, timestamp), OperatorName);
                }
                else
                {
                    _eventTime.ReportLateEvent(item, timestamp, new DateTime(watermarkTicks, DateTimeKind.Utc), OperatorName);
                }
            }
        }

        // Called under _lock: merges the event with every session its interval overlaps, then runs the trigger
        private void AddToSessions(TInput item, string key, DateTime timestamp, long watermarkTicks)
        {
            var overlapping = new List<(string Key, AdvancedSessionState<TInput> Session)>();
            if (_sessionsByKey.TryGetValue(key, out var existingKeys))
            {
                foreach (var existingKey in existingKeys.ToList())
                {
                    var existing = _stateStore.Get(existingKey);

                    // Sessions of one key never overlap each other, so comparing with the event's interval is enough
                    if (existing != null
                        && timestamp <= existing.LastActivityTime + _inactivityGap
                        && existing.StartTime <= timestamp + _inactivityGap)
                    {
                        overlapping.Add((existingKey, existing));
                    }
                }
            }

            string sessionKey;
            AdvancedSessionState<TInput> session;
            var wasFired = overlapping.Any(o => _firedSessions.Contains(o.Key));

            if (overlapping.Count == 1)
            {
                // Extend the one session in place, keeping its trigger and emission state
                (sessionKey, session) = overlapping[0];
                session.Items.Add(item);
                if (timestamp < session.StartTime) session.StartTime = timestamp;
                if (timestamp > session.LastActivityTime) session.LastActivityTime = timestamp;
            }
            else
            {
                var ordered = overlapping.OrderBy(o => o.Session.StartTime).ToList();
                foreach (var o in ordered)
                {
                    RemoveEventTimeSession(o.Key);
                }

                // A unique key: a durable store may still hold sessions of an earlier run
                sessionKey = $"session_{key}_{Guid.NewGuid():N}";
                var newKey = sessionKey;
                var items = ordered.SelectMany(o => o.Session.Items).ToList();
                items.Add(item);
                session = new AdvancedSessionState<TInput>
                {
                    Key = key,
                    StartTime = ordered.Count > 0 ? new[] { timestamp, ordered[0].Session.StartTime }.Min() : timestamp,
                    LastActivityTime = ordered.Count > 0 ? ordered.Max(o => o.Session.LastActivityTime) : timestamp,
                    Items = items,
                    TriggerContext = new TriggerContext<TInput>(newKey, () => _stateStore.Get(newKey)?.Items?.Count ?? 0, CurrentTime),
                    EmissionSequence = ordered.Count > 0 ? ordered.Max(o => o.Session.EmissionSequence) : 0,
                    LastEmittedItems = ordered.SelectMany(o => o.Session.LastEmittedItems).ToList()
                };
                if (session.LastActivityTime < timestamp) session.LastActivityTime = timestamp;

                if (!_sessionsByKey.TryGetValue(key, out existingKeys))
                {
                    existingKeys = new List<string>();
                    _sessionsByKey[key] = existingKeys;
                }
                existingKeys.Add(sessionKey);
            }

            _stateStore.Put(sessionKey, session);

            var windowEnd = session.LastActivityTime + _inactivityGap;
            if (windowEnd.Ticks < watermarkTicks)
            {
                // Joined a session the watermark has passed, within the allowed lateness: a late result
                EmitSession(sessionKey, session, isFinal: true, WindowEmissionType.Late);
                _firedSessions.Add(sessionKey);
                return;
            }

            if (wasFired)
            {
                // Extended past the watermark again: the session is open once more
                _firedSessions.Remove(sessionKey);
            }

            var triggerResult = _config.Trigger.OnElement(item, timestamp, session.StartTime, windowEnd, session.TriggerContext);
            if (triggerResult == TriggerResult.Fire)
            {
                EmitSession(sessionKey, session, isFinal: false);
            }
            else if (triggerResult == TriggerResult.FireAndPurge)
            {
                EmitSession(sessionKey, session, isFinal: true);
                RemoveEventTimeSession(sessionKey);
            }
        }

        // The trigger's notion of now: the wall clock, or the watermark in event time
        private DateTime CurrentTime()
        {
            return _eventTime != null ? new DateTime(_watermarkTicks, DateTimeKind.Utc) : DateTime.UtcNow;
        }

        // Called under _lock
        private void RemoveEventTimeSession(string sessionKey)
        {
            var session = _stateStore.Get(sessionKey);
            _stateStore.Remove(sessionKey);
            _firedSessions.Remove(sessionKey);

            if (session != null && _sessionsByKey.TryGetValue(session.Key, out var sessionKeys))
            {
                sessionKeys.Remove(sessionKey);
                if (sessionKeys.Count == 0)
                {
                    _sessionsByKey.Remove(session.Key);
                }
            }
        }

        /// <inheritdoc />
        public void EnableEventTime(EventTimeContext context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            lock (_lock)
            {
                var lateness = _config.AllowedLateness > TimeSpan.Zero ? _config.AllowedLateness : context.AllowedLateness;
                _latenessTicks = Math.Min(lateness.Ticks, WatermarkStrategy.MaxLateness.Ticks);
                _eventTime = context;

                // Sessions close by watermark from now on
                _sessionTimer?.Dispose();
                _sessionTimer = null;

                // Adopt sessions that are already open
                foreach (var kvp in _stateStore.GetAll().ToList())
                {
                    if (!_sessionsByKey.TryGetValue(kvp.Value.Key, out var sessionKeys))
                    {
                        sessionKeys = new List<string>();
                        _sessionsByKey[kvp.Value.Key] = sessionKeys;
                    }
                    sessionKeys.Add(kvp.Key);
                }
            }
        }

        /// <inheritdoc />
        public void OnWatermark(DateTime watermark)
        {
            lock (_lock)
            {
                if (_eventTime == null || watermark.Ticks <= _watermarkTicks)
                    return;

                _watermarkTicks = watermark.Ticks;

                var sessions = _sessionsByKey.Values
                    .SelectMany(keys => keys)
                    .Select(sessionKey => (Key: sessionKey, Session: _stateStore.Get(sessionKey)))
                    .Where(s => s.Session != null)
                    .OrderBy(s => s.Session.LastActivityTime)
                    .ThenBy(s => s.Session.Key, StringComparer.Ordinal)
                    .ToList();

                foreach (var (sessionKey, session) in sessions)
                {
                    var windowEnd = session.LastActivityTime + _inactivityGap;

                    if (_firedSessions.Contains(sessionKey))
                    {
                        if (windowEnd.Ticks + _latenessTicks < _watermarkTicks)
                        {
                            RemoveEventTimeSession(sessionKey);
                        }
                        continue;
                    }

                    // An event at exactly last activity + gap still joins, so the trigger sees the end one tick later
                    var triggerResult = _config.Trigger.OnProcessingTime(watermark, session.StartTime, windowEnd.AddTicks(1), session.TriggerContext);
                    if (triggerResult == TriggerResult.FireAndPurge || windowEnd.Ticks < _watermarkTicks)
                    {
                        EmitSession(sessionKey, session, isFinal: true);
                        if (windowEnd.Ticks + _latenessTicks >= _watermarkTicks)
                        {
                            _firedSessions.Add(sessionKey);
                        }
                        else
                        {
                            RemoveEventTimeSession(sessionKey);
                        }
                    }
                    else if (triggerResult == TriggerResult.Fire)
                    {
                        EmitSession(sessionKey, session, isFinal: false);
                    }
                }
            }
        }

        private void EmitSession(string sessionKey, AdvancedSessionState<TInput> session, bool isFinal, WindowEmissionType? finalType = null)
        {
            if (session == null || session.Items.Count == 0)
            {
                return;
            }

            session.EmissionSequence++;
            var emissionType = isFinal ? finalType ?? WindowEmissionType.OnTime : WindowEmissionType.Early;
            var windowEnd = session.LastActivityTime + _inactivityGap;

            // Handle state mode
            List<TInput> itemsToEmit;
            switch (_config.StateMode)
            {
                case WindowStateMode.Accumulating:
                    itemsToEmit = new List<TInput>(session.Items);
                    break;

                case WindowStateMode.AccumulatingAndRetracting:
                    // First emit retraction for previous result if there was one
                    if (session.LastEmittedItems.Count > 0)
                    {
                        var retractionResult = new WindowResult<string, TInput>(
                            session.Key,
                            session.StartTime,
                            windowEnd,
                            session.LastEmittedItems,
                            WindowEmissionType.Retraction,
                            false,
                            DateTime.UtcNow,
                            session.EmissionSequence - 1);
                        _nextOperator?.Process(retractionResult);
                    }
                    itemsToEmit = new List<TInput>(session.Items);
                    session.LastEmittedItems = new List<TInput>(itemsToEmit);
                    break;

                case WindowStateMode.Discarding:
                default:
                    // Only emit items since last emission
                    if (session.LastEmittedItems.Count > 0)
                    {
                        itemsToEmit = session.Items.Skip(session.LastEmittedItems.Count).ToList();
                    }
                    else
                    {
                        itemsToEmit = new List<TInput>(session.Items);
                    }
                    session.LastEmittedItems = new List<TInput>(session.Items);
                    break;
            }

            if (itemsToEmit.Count > 0)
            {
                var windowResult = new WindowResult<string, TInput>(
                    session.Key,
                    session.StartTime,
                    windowEnd,
                    itemsToEmit,
                    emissionType,
                    isFinal,
                    DateTime.UtcNow,
                    session.EmissionSequence);

                _nextOperator?.Process(windowResult);
            }
        }

        private void EvaluateSessions(object state)
        {
            var now = DateTime.UtcNow;
            List<string> expiredSessions = new List<string>();

            lock (_lock)
            {
                // A tick already running when event time was enabled
                if (_eventTime != null)
                    return;

                foreach (var kvp in _stateStore.GetAll())
                {
                    var session = kvp.Value;
                    var timeSinceLastActivity = now - session.LastActivityTime;
                    var windowEnd = session.LastActivityTime + _inactivityGap;

                    // Evaluate processing time trigger
                    var triggerResult = _config.Trigger.OnProcessingTime(now, session.StartTime, windowEnd, session.TriggerContext);

                    if (triggerResult == TriggerResult.Fire)
                    {
                        EmitSession(kvp.Key, session, isFinal: false);
                    }
                    else if (triggerResult == TriggerResult.FireAndPurge || timeSinceLastActivity > _inactivityGap)
                    {
                        expiredSessions.Add(kvp.Key);
                    }
                }

                foreach (var sessionKey in expiredSessions)
                {
                    var session = _stateStore.Get(sessionKey);
                    if (session != null)
                    {
                        EmitSession(sessionKey, session, isFinal: true);
                        _stateStore.Remove(sessionKey);
                    }
                }
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;

            // Propagate telemetry
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled && _telemetryProvider != null)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IDataStore> GetStateStores()
        {
            yield return _stateStore;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _sessionTimer?.Dispose();
                    _sessionTimer = null;
                }
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Represents the state of an advanced session window with trigger support.
    /// </summary>
    /// <typeparam name="TInput">The type of items in the session.</typeparam>
    public class AdvancedSessionState<TInput>
    {
        /// <summary>
        /// Gets or sets the key that identifies this session.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Gets or sets the start time of the session.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the time of the last activity in the session.
        /// </summary>
        public DateTime LastActivityTime { get; set; }

        /// <summary>
        /// Gets or sets the items in the session.
        /// </summary>
        public List<TInput> Items { get; set; }

        /// <summary>
        /// Gets or sets the trigger context for this session.
        /// </summary>
        public TriggerContext<TInput> TriggerContext { get; set; }

        /// <summary>
        /// Gets or sets the emission sequence number.
        /// </summary>
        public int EmissionSequence { get; set; }

        /// <summary>
        /// Gets or sets the last emitted items (for state mode tracking).
        /// </summary>
        public List<TInput> LastEmittedItems { get; set; }
    }
}
