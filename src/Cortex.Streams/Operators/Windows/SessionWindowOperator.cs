using Cortex.States;
using Cortex.States.Operators;
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
    /// A window operator that groups items into session windows based on inactivity gaps.
    /// A new session window is created when the gap between events exceeds the configured inactivity gap.
    /// </summary>
    /// <remarks>
    /// By default sessions close by the wall clock. Downstream of <c>WithWatermarks</c> they work in event time:
    /// every event covers <c>[timestamp, timestamp + gap]</c>, and events whose intervals overlap belong to one session,
    /// so an out-of-order event can extend a session backwards or merge two sessions, and a key can have several open
    /// sessions. A session fires when the watermark passes its last activity plus the gap, fires again when an event
    /// within the allowed lateness joins it, and is removed when the watermark passes its end plus the allowed lateness.
    /// An event whose own interval ended more than the allowed lateness before the watermark is dropped and reported.
    /// </remarks>
    /// <typeparam name="TInput">The type of the input items.</typeparam>
    /// <typeparam name="TKey">The type of the key used to partition sessions.</typeparam>
    public class SessionWindowOperator<TInput, TKey> : IOperator, IStatefulOperator, ITelemetryEnabled, IWatermarkAware, IDisposable
    {
        private static readonly string OperatorName = $"SessionWindowOperator<{typeof(TInput).Name}>";

        private readonly Func<TInput, TKey> _keySelector;
        private readonly Func<TInput, DateTime> _timestampSelector;
        private readonly TimeSpan _inactivityGap;
        private readonly IDataStore<string, SessionState<TInput>> _stateStore;
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
        private long _nextDeadlineTicks = long.MaxValue;
        private long _nextSessionId;
        private readonly Dictionary<string, List<string>> _sessionsByKey = new Dictionary<string, List<string>>();
        private readonly HashSet<string> _firedSessions = new HashSet<string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="SessionWindowOperator{TInput, TKey}"/> class.
        /// </summary>
        /// <param name="keySelector">A function to extract the key from each input item.</param>
        /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
        /// <param name="inactivityGap">The duration of inactivity after which a session is closed.</param>
        /// <param name="stateStore">The state store to use for storing session data.</param>
        public SessionWindowOperator(
            Func<TInput, TKey> keySelector,
            Func<TInput, DateTime> timestampSelector,
            TimeSpan inactivityGap,
            IDataStore<string, SessionState<TInput>> stateStore)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _timestampSelector = timestampSelector ?? throw new ArgumentNullException(nameof(timestampSelector));
            _inactivityGap = inactivityGap;
            _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));

            // Start session evaluation timer
            _sessionTimer = new Timer(EvaluateSessions, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"session_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by SessionWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"session_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for SessionWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"SessionWindowOperator_{typeof(TInput).Name}");

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
                using (var span = _tracer.StartSpan("SessionWindowOperator.Process"))
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
                    session = new SessionState<TInput>
                    {
                        Key = key.ToString(),
                        StartTime = timestamp,
                        LastActivityTime = timestamp,
                        Items = new List<TInput> { typedInput }
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
                        EmitSession(sessionKey, session);

                        // Start a new session
                        session = new SessionState<TInput>
                        {
                            Key = key.ToString(),
                            StartTime = timestamp,
                            LastActivityTime = timestamp,
                            Items = new List<TInput> { typedInput }
                        };
                        _stateStore.Put(sessionKey, session);
                    }
                    else
                    {
                        // Extend the current session
                        session.Items.Add(typedInput);
                        session.LastActivityTime = timestamp;
                        _stateStore.Put(sessionKey, session);
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
                _eventTime.ReportLateEvent(item, timestamp, new DateTime(watermarkTicks, DateTimeKind.Utc), OperatorName);
            }
        }

        // Called under _lock: merges the event with every session its interval overlaps
        private void AddToSessions(TInput item, string key, DateTime timestamp, long watermarkTicks)
        {
            if (!_sessionsByKey.TryGetValue(key, out var sessionKeys))
            {
                sessionKeys = new List<string>();
                _sessionsByKey[key] = sessionKeys;
            }

            var start = timestamp;
            var lastActivity = timestamp;
            var overlapping = new List<SessionState<TInput>>();

            foreach (var sessionKey in sessionKeys.ToList())
            {
                var session = _stateStore.Get(sessionKey);
                if (session == null)
                    continue;

                // Sessions of one key never overlap each other, so comparing with the event's interval is enough
                if (timestamp <= session.LastActivityTime + _inactivityGap && session.StartTime <= timestamp + _inactivityGap)
                {
                    overlapping.Add(session);
                    if (session.StartTime < start) start = session.StartTime;
                    if (session.LastActivityTime > lastActivity) lastActivity = session.LastActivityTime;
                    RemoveSession(sessionKey);
                }
            }

            var items = overlapping.OrderBy(s => s.StartTime).SelectMany(s => s.Items).ToList();
            items.Add(item);

            var merged = new SessionState<TInput>
            {
                Key = key,
                StartTime = start,
                LastActivityTime = lastActivity,
                Items = items
            };

            var mergedKey = $"session_{key}_{++_nextSessionId}";
            if (!_sessionsByKey.TryGetValue(key, out sessionKeys))
            {
                sessionKeys = new List<string>();
                _sessionsByKey[key] = sessionKeys;
            }
            sessionKeys.Add(mergedKey);
            _stateStore.Put(mergedKey, merged);

            var endTicks = (lastActivity + _inactivityGap).Ticks;
            if (endTicks < watermarkTicks)
            {
                // Within the allowed lateness of a session the watermark has passed: fire it again
                FireSession(mergedKey, merged, retain: true);
            }
            else
            {
                _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, endTicks + 1);
            }
        }

        /// <inheritdoc />
        public void EnableEventTime(EventTimeContext context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            lock (_lock)
            {
                _latenessTicks = context.AllowedLateness.Ticks;
                Volatile.Write(ref _eventTime, context);

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
                    _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, Deadline(kvp.Key, kvp.Value));
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
                if (_watermarkTicks < _nextDeadlineTicks)
                    return;

                var due = _sessionsByKey.Values
                    .SelectMany(keys => keys)
                    .Select(sessionKey => (Key: sessionKey, Session: _stateStore.Get(sessionKey)))
                    .Where(s => s.Session != null && Deadline(s.Key, s.Session) <= _watermarkTicks)
                    .OrderBy(s => s.Session.LastActivityTime)
                    .ThenBy(s => s.Key, StringComparer.Ordinal)
                    .ToList();

                foreach (var (sessionKey, session) in due)
                {
                    if (_firedSessions.Contains(sessionKey))
                    {
                        RemoveSession(sessionKey);
                    }
                    else
                    {
                        var endTicks = (session.LastActivityTime + _inactivityGap).Ticks;
                        FireSession(sessionKey, session, retain: endTicks + _latenessTicks >= _watermarkTicks);
                    }
                }

                _nextDeadlineTicks = long.MaxValue;
                foreach (var sessionKey in _sessionsByKey.Values.SelectMany(keys => keys))
                {
                    var session = _stateStore.Get(sessionKey);
                    if (session != null)
                    {
                        _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, Deadline(sessionKey, session));
                    }
                }
            }
        }

        // The first watermark that closes a session: past its end when open, past its end plus lateness once fired
        private long Deadline(string sessionKey, SessionState<TInput> session)
        {
            var endTicks = (session.LastActivityTime + _inactivityGap).Ticks;
            return (_firedSessions.Contains(sessionKey) ? endTicks + _latenessTicks : endTicks) + 1;
        }

        // Called under _lock
        private void FireSession(string sessionKey, SessionState<TInput> session, bool retain)
        {
            if (session.Items.Count > 0)
            {
                // A kept session still changes, so its result gets a copy of the items
                var items = retain ? new List<TInput>(session.Items) : session.Items;
                _nextOperator?.Process(new WindowResult<string, TInput>(
                    session.Key,
                    session.StartTime,
                    session.LastActivityTime + _inactivityGap,
                    items));
            }

            if (retain)
            {
                _firedSessions.Add(sessionKey);
                _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, Deadline(sessionKey, session));
            }
            else
            {
                RemoveSession(sessionKey);
            }
        }

        // Called under _lock
        private void RemoveSession(string sessionKey)
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

        private void EmitSession(string sessionKey, SessionState<TInput> session)
        {
            if (session != null && session.Items.Count > 0)
            {
                var windowResult = new WindowResult<string, TInput>(
                    session.Key,
                    session.StartTime,
                    session.LastActivityTime + _inactivityGap,
                    session.Items);

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

                    if (timeSinceLastActivity > _inactivityGap)
                    {
                        expiredSessions.Add(kvp.Key);
                    }
                }

                foreach (var sessionKey in expiredSessions)
                {
                    var session = _stateStore.Get(sessionKey);
                    if (session != null)
                    {
                        EmitSession(sessionKey, session);
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
    /// Represents the state of a session window.
    /// </summary>
    /// <typeparam name="TInput">The type of items in the session.</typeparam>
    public class SessionState<TInput>
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
    }
}
