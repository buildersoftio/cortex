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
    /// A window operator that groups items into fixed-size, non-overlapping time windows.
    /// Each window has a fixed duration, and windows do not overlap.
    /// </summary>
    /// <remarks>
    /// By default a window closes when the wall clock passes its end. Downstream of <c>WithWatermarks</c> it works in
    /// event time: a window fires when the watermark reaches its end, fires again for each event that arrives within
    /// the allowed lateness, and is removed when the watermark reaches its end plus the allowed lateness; later events
    /// are dropped and reported as late.
    /// </remarks>
    /// <typeparam name="TInput">The type of the input items.</typeparam>
    /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
    public class TumblingWindowOperator<TInput, TKey> : IOperator, IStatefulOperator, ITelemetryEnabled, IWatermarkAware, IWatermarkDeadline, IDisposable
    {
        private static readonly string OperatorName = $"TumblingWindowOperator<{typeof(TInput).Name}>";

        private readonly Func<TInput, TKey> _keySelector;
        private readonly Func<TInput, DateTime> _timestampSelector;
        private readonly TimeSpan _windowSize;
        private readonly IDataStore<string, List<TInput>> _stateStore;
        private readonly Dictionary<string, DateTime> _windowEndTimes;
        private readonly object _lock = new object();
        private IOperator _nextOperator;
        private Timer _windowTimer;
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
        private readonly HashSet<string> _firedWindows = new HashSet<string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="TumblingWindowOperator{TInput, TKey}"/> class.
        /// </summary>
        /// <param name="keySelector">A function to extract the key from each input item.</param>
        /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
        /// <param name="windowSize">The size of each tumbling window.</param>
        /// <param name="stateStore">The state store to use for storing window data.</param>
        public TumblingWindowOperator(
            Func<TInput, TKey> keySelector,
            Func<TInput, DateTime> timestampSelector,
            TimeSpan windowSize,
            IDataStore<string, List<TInput>> stateStore)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _timestampSelector = timestampSelector ?? throw new ArgumentNullException(nameof(timestampSelector));
            _windowSize = windowSize;
            _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
            _windowEndTimes = new Dictionary<string, DateTime>();

            // Start window evaluation timer
            _windowTimer = new Timer(EvaluateWindows, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"tumbling_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by TumblingWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"tumbling_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for TumblingWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"TumblingWindowOperator_{typeof(TInput).Name}");

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
                using (var span = _tracer.StartSpan("TumblingWindowOperator.Process"))
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

            // Calculate window boundaries
            var windowStart = GetWindowStart(timestamp);
            var windowEnd = windowStart + _windowSize;
            var windowKey = GetWindowKey(key, windowStart);

            if (Volatile.Read(ref _eventTime) != null)
            {
                ProcessEventTime(typedInput, timestamp, windowKey, windowEnd);
                return;
            }

            lock (_lock)
            {
                var windowItems = _stateStore.Get(windowKey) ?? new List<TInput>();
                windowItems.Add(typedInput);
                _stateStore.Put(windowKey, windowItems);

                if (!_windowEndTimes.ContainsKey(windowKey))
                {
                    _windowEndTimes[windowKey] = windowEnd;
                }
            }
        }

        private void ProcessEventTime(TInput item, DateTime timestamp, string windowKey, DateTime windowEnd)
        {
            bool late;
            long watermarkTicks;

            lock (_lock)
            {
                // Watermarks no window needed were not sent, so the stream's own may be ahead of ours
                watermarkTicks = Math.Max(_watermarkTicks, _eventTime.CurrentWatermarkTicks);
                late = windowEnd.Ticks + _latenessTicks <= watermarkTicks;

                if (!late)
                {
                    var windowItems = _stateStore.Get(windowKey) ?? new List<TInput>();
                    windowItems.Add(item);
                    _stateStore.Put(windowKey, windowItems);
                    _windowEndTimes[windowKey] = windowEnd;

                    if (windowEnd.Ticks <= watermarkTicks)
                    {
                        // Within the allowed lateness of a window the watermark has passed: fire it again
                        FireWindow(windowKey, retain: true);
                    }
                    else
                    {
                        _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, windowEnd.Ticks);
                    }
                }
            }

            if (late)
            {
                _eventTime.ReportLateEvent(item, timestamp, new DateTime(watermarkTicks, DateTimeKind.Utc), OperatorName);
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

                // Windows close by watermark from now on
                _windowTimer?.Dispose();
                _windowTimer = null;

                foreach (var windowEnd in _windowEndTimes.Values)
                {
                    _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, windowEnd.Ticks);
                }
            }
        }

        long IWatermarkDeadline.NextWatermarkDeadlineTicks => Volatile.Read(ref _nextDeadlineTicks);

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

                var due = _windowEndTimes
                    .Where(kvp => Deadline(kvp.Key, kvp.Value) <= _watermarkTicks)
                    .OrderBy(kvp => kvp.Value)
                    .ThenBy(kvp => kvp.Key, StringComparer.Ordinal)
                    .ToList();

                foreach (var kvp in due)
                {
                    if (_firedWindows.Contains(kvp.Key))
                    {
                        RemoveWindow(kvp.Key);
                    }
                    else
                    {
                        FireWindow(kvp.Key, retain: kvp.Value.Ticks + _latenessTicks > _watermarkTicks);
                    }
                }

                _nextDeadlineTicks = long.MaxValue;
                foreach (var kvp in _windowEndTimes)
                {
                    _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, Deadline(kvp.Key, kvp.Value));
                }
            }
        }

        // An open window is due at its end; a fired one is kept until its end plus the allowed lateness
        private long Deadline(string windowKey, DateTime windowEnd)
        {
            return _firedWindows.Contains(windowKey) ? windowEnd.Ticks + _latenessTicks : windowEnd.Ticks;
        }

        // Called under _lock
        private void FireWindow(string windowKey, bool retain)
        {
            var windowEnd = _windowEndTimes[windowKey];
            var windowItems = _stateStore.Get(windowKey);

            if (windowItems != null && windowItems.Count > 0)
            {
                var keyString = windowKey.Substring(0, windowKey.LastIndexOf('_'));

                // A kept window still changes, so its result gets a copy of the items
                var emitted = retain ? new List<TInput>(windowItems) : windowItems;
                _nextOperator?.Process(new WindowResult<string, TInput>(keyString, windowEnd - _windowSize, windowEnd, emitted));
            }

            if (retain)
            {
                _firedWindows.Add(windowKey);
                _nextDeadlineTicks = Math.Min(_nextDeadlineTicks, windowEnd.Ticks + _latenessTicks);
            }
            else
            {
                RemoveWindow(windowKey);
            }
        }

        // Called under _lock
        private void RemoveWindow(string windowKey)
        {
            _stateStore.Remove(windowKey);
            _windowEndTimes.Remove(windowKey);
            _firedWindows.Remove(windowKey);
        }

        private DateTime GetWindowStart(DateTime timestamp)
        {
            var ticks = timestamp.Ticks;
            var windowTicks = _windowSize.Ticks;
            var windowStartTicks = (ticks / windowTicks) * windowTicks;
            return new DateTime(windowStartTicks, timestamp.Kind);
        }

        private string GetWindowKey(TKey key, DateTime windowStart)
        {
            return $"{key}_{windowStart.Ticks}";
        }

        private void EvaluateWindows(object state)
        {
            var now = DateTime.UtcNow;
            List<string> expiredWindows = new List<string>();

            lock (_lock)
            {
                // A tick already running when event time was enabled
                if (_eventTime != null)
                    return;

                foreach (var kvp in _windowEndTimes)
                {
                    if (now >= kvp.Value)
                    {
                        expiredWindows.Add(kvp.Key);
                    }
                }

                foreach (var windowKey in expiredWindows)
                {
                    var windowItems = _stateStore.Get(windowKey);
                    if (windowItems != null && windowItems.Count > 0)
                    {
                        var windowEnd = _windowEndTimes[windowKey];
                        var windowStart = windowEnd - _windowSize;

                        // Parse the key from the window key
                        var keyEndIndex = windowKey.LastIndexOf('_');
                        var keyString = windowKey.Substring(0, keyEndIndex);

                        // Create window result
                        var windowResult = new WindowResult<string, TInput>(
                            keyString,
                            windowStart,
                            windowEnd,
                            windowItems);

                        // Emit the window result
                        _nextOperator?.Process(windowResult);

                        // Clean up
                        _stateStore.Remove(windowKey);
                    }

                    _windowEndTimes.Remove(windowKey);
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
                    _windowTimer?.Dispose();
                    _windowTimer = null;
                }
                _disposed = true;
            }
        }
    }
}
