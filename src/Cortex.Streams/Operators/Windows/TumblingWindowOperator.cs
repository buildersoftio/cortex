using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Abstractions.FaultTolerance;
using Cortex.Streams.Windows;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Timers;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator implementing tumbling windows. It uses an external
    /// <see cref="IDataStore{WindowKey, List{TInput}}"/> to keep track of active windows.
    /// An operator implementing tumbling windows, with a background timer that
    /// periodically checks whether any window has expired. This ensures windows
    /// are closed even if no new events arrive.
    /// 
    /// You can choose:
    ///   - <b>Processing-time</b> (simple wall-clock checks).
    ///   - <b>Event-time</b> (with watermarks) by specifying <c>useEventTime=true</c>
    ///     and providing <c>eventTimeExtractor</c> and <c>allowedLateness</c>.
    /// 
    /// Additionally, the operator can:
    ///   - <b>Checkpoint</b> its boundary fields (e.g., <c>_maxEventTime</c>)
    ///     so you can recover after a crash.
    ///   - <b>Audit</b> final (closed) windows by storing them in <c>_auditStore</c>.
    /// 
    /// The operator emits a <c>List&lt;TInput&gt;</c> batch each time a window closes.
    /// </summary>
    /// <typeparam name="TInput">Type of input records that go through the operator.</typeparam>
    public class TumblingWindowOperator<TInput>
           : IOperator, IStatefulOperator, ITelemetryEnabled, ICheckpointTable
    {
        private readonly TimeSpan _windowSize;
        private readonly IDataStore<WindowKey, List<TInput>> _activeWindowsStore;
        private readonly bool _useEventTime;
        private readonly Func<TInput, DateTimeOffset> _eventTimeExtractor;
        private readonly TimeSpan _allowedLateness;

        private readonly bool _storeResultsForAudit;
        private readonly IDataStore<WindowKey, List<TInput>> _auditStore;

        // boundary fields
        private DateTimeOffset _maxEventTime;
        private DateTimeOffset _currentProcWindowStart;
        private DateTimeOffset _currentProcWindowEnd;

        private readonly bool _enableCheckpointing;
        private readonly IDataStore<string, TumblingCheckpointState> _checkpointStore;
        private const string CHECKPOINT_KEY = "TumblingWindowCheckpoint";

        // concurrency lock
        private readonly object _syncLock = new object();

        private IOperator _nextOperator;

        // telemetry
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // NEW: A background timer that checks for expired windows
        private readonly Timer _closingTimer;

        /// <summary>
        /// Constructs a TumblingWindowOperator that references an external <paramref name="activeWindowsStore"/>
        /// for in-progress windows, and spawns a background timer to close windows periodically.
        /// </summary>
        /// <param name="windowSize">Duration for each tumbling window.</param>
        /// <param name="activeWindowsStore">
        ///   A store for open windows (keys=WindowKey, values=List of TInput). Must not be null.
        /// </param>
        /// <param name="useEventTime">True => event-time/watermarks; false => processing-time.</param>
        /// <param name="eventTimeExtractor">
        ///   Extracts a timestamp from each event if <paramref name="useEventTime"/> is true.
        /// </param>
        /// <param name="allowedLateness">
        ///   For event-time, how late events can arrive. Watermark=(_maxEventTime - allowedLateness).
        /// </param>
        /// <param name="storeResultsForAudit">
        ///   If true, final window content is saved to <paramref name="auditStore"/>.
        /// </param>
        /// <param name="auditStore">
        ///   Where final windows are persisted if <paramref name="storeResultsForAudit"/> is true.
        /// </param>
        /// <param name="enableCheckpointing">
        ///   If true, boundary fields (like _maxEventTime or _currentProcWindowEnd) are stored in <paramref name="checkpointStore"/>.
        /// </param>
        /// <param name="checkpointStore">
        ///   A data store used for checkpointing if <paramref name="enableCheckpointing"/> is true.
        /// </param>
        /// <param name="closingTimerIntervalMs">
        ///   Interval (milliseconds) for the background timer. Default=1000 ms (1s).
        ///   Adjust for how frequently you want to check expired windows.
        /// </param>
        public TumblingWindowOperator(
            TimeSpan windowSize,
            IDataStore<WindowKey, List<TInput>> activeWindowsStore,
            bool useEventTime = false,
            Func<TInput, DateTimeOffset> eventTimeExtractor = null,
            TimeSpan? allowedLateness = null,
            bool storeResultsForAudit = false,
            IDataStore<WindowKey, List<TInput>> auditStore = null,
            bool enableCheckpointing = false,
            IDataStore<string, TumblingCheckpointState> checkpointStore = null,
            double closingTimerIntervalMs = 1000
        )
        {
            if (activeWindowsStore == null)
                throw new ArgumentNullException(nameof(activeWindowsStore), "Must provide a store for active windows.");

            _windowSize = windowSize;
            _activeWindowsStore = activeWindowsStore;
            _useEventTime = useEventTime;
            _eventTimeExtractor = eventTimeExtractor;
            _allowedLateness = allowedLateness ?? TimeSpan.Zero;

            _storeResultsForAudit = storeResultsForAudit;
            _auditStore = auditStore;
            if (_storeResultsForAudit && _auditStore == null)
            {
                throw new ArgumentException("If storeResultsForAudit=true, auditStore cannot be null.");
            }

            _enableCheckpointing = enableCheckpointing;
            _checkpointStore = checkpointStore;
            if (_enableCheckpointing && _checkpointStore == null)
            {
                throw new ArgumentException("enableCheckpointing=true requires a non-null checkpointStore");
            }

            if (_useEventTime && _eventTimeExtractor == null)
                throw new ArgumentException("When useEventTime=true, you must supply an eventTimeExtractor.");

            if (!_useEventTime)
            {
                // define initial processing-time boundaries
                _currentProcWindowStart = DateTimeOffset.UtcNow;
                _currentProcWindowEnd = _currentProcWindowStart.Add(_windowSize);
            }

            // if checkpointing is on, restore boundary fields
            if (_enableCheckpointing)
            {
                RestoreCheckpoint();
            }

            // Start the background timer to forcibly close windows if no new events arrive
            _closingTimer = new Timer(closingTimerIntervalMs);
            _closingTimer.AutoReset = true;
            _closingTimer.Elapsed += (s, e) => CloseExpiredWindows();
            _closingTimer.Start();
        }

        /// <summary>
        /// The background timer calls this method to close any expired windows.
        /// If no new events arrive, we still check for windows that have reached
        /// their end time (processing-time) or have a watermark >= window end (event-time).
        /// </summary>
        private void CloseExpiredWindows()
        {
            lock (_syncLock)
            {
                if (!_useEventTime)
                {
                    // For processing-time, check if the current clock passes the current window boundary
                    var now = DateTimeOffset.UtcNow;
                    if (now >= _currentProcWindowEnd)
                    {
                        // close the current window
                        var wKey = new WindowKey(_currentProcWindowStart, _currentProcWindowEnd);
                        EmitWindowAndRemove(wKey);

                        // define next window
                        _currentProcWindowStart = _currentProcWindowEnd;
                        _currentProcWindowEnd = _currentProcWindowStart.Add(_windowSize);

                        if (_enableCheckpointing)
                            Checkpoint();
                    }
                }
                else
                {
                    // For event-time, we compute a watermark if we haven't seen new events.
                    var watermark = _maxEventTime - _allowedLateness;

                    // close any windows whose end <= watermark
                    var endedWindowKeys = new List<WindowKey>();
                    foreach (var kvp in _activeWindowsStore.GetAll())
                    {
                        if (kvp.Key.WindowEnd <= watermark)
                        {
                            endedWindowKeys.Add(kvp.Key);
                        }
                    }
                    foreach (var key in endedWindowKeys)
                    {
                        EmitWindowAndRemove(key);
                    }

                    if (_enableCheckpointing && endedWindowKeys.Count > 0)
                        Checkpoint();
                }
            }
        }

        public void Process(object input)
        {
            if (_telemetryProvider != null)
            {
                var sw = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("TumblingWindowOperator.Process"))
                {
                    try
                    {
                        lock (_syncLock)
                        {
                            if (_useEventTime)
                                HandleEventTimeWindow((TInput)input);
                            else
                                HandleProcessingTimeWindow((TInput)input);

                            // If checkpointing, we can checkpoint after each record
                            if (_enableCheckpointing)
                                Checkpoint();
                        }

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
                        sw.Stop();
                        _recordProcessingTime?.Invoke(sw.Elapsed.TotalMilliseconds);
                        _incrementProcessedCounter?.Invoke();
                    }
                }
            }
            else
            {
                lock (_syncLock)
                {
                    if (_useEventTime)
                        HandleEventTimeWindow((TInput)input);
                    else
                        HandleProcessingTimeWindow((TInput)input);

                    if (_enableCheckpointing)
                        Checkpoint();
                }
            }
        }

        #region Processing-Time Logic

        private void HandleProcessingTimeWindow(TInput item)
        {
            var wKey = new WindowKey(_currentProcWindowStart, _currentProcWindowEnd);

            var batch = _activeWindowsStore.Get(wKey) ?? new List<TInput>();
            batch.Add(item);
            _activeWindowsStore.Put(wKey, batch);

            // If "real" time surpasses boundary, close now (in addition to the background timer)
            var now = DateTimeOffset.UtcNow;
            if (now >= _currentProcWindowEnd)
            {
                EmitWindowAndRemove(wKey);
                _currentProcWindowStart = _currentProcWindowEnd;
                _currentProcWindowEnd = _currentProcWindowStart.Add(_windowSize);
            }
        }

        #endregion

        #region Event-Time Logic

        private void HandleEventTimeWindow(TInput item)
        {
            var eventTime = _eventTimeExtractor(item);
            if (eventTime > _maxEventTime)
                _maxEventTime = eventTime;

            var windowStart = FloorToWindowStart(eventTime, _windowSize);
            var windowEnd = windowStart.Add(_windowSize);
            var wKey = new WindowKey(windowStart, windowEnd);

            var batch = _activeWindowsStore.Get(wKey) ?? new List<TInput>();
            batch.Add(item);
            _activeWindowsStore.Put(wKey, batch);

            // close any windows that ended => windowEnd <= watermark
            var watermark = _maxEventTime - _allowedLateness;
            var endedWindowKeys = new List<WindowKey>();
            foreach (var kvp in _activeWindowsStore.GetAll())
            {
                if (kvp.Key.WindowEnd <= watermark)
                    endedWindowKeys.Add(kvp.Key);
            }
            foreach (var key in endedWindowKeys)
            {
                EmitWindowAndRemove(key);
            }
        }

        private static DateTimeOffset FloorToWindowStart(DateTimeOffset time, TimeSpan windowSize)
        {
            var ticks = time.Ticks / windowSize.Ticks * windowSize.Ticks;
            return new DateTimeOffset(ticks, time.Offset);
        }

        #endregion

        /// <summary>
        /// Closes the specified window: emits the batch to the next operator,
        /// optionally audits it, then removes it from the active store.
        /// </summary>
        private void EmitWindowAndRemove(WindowKey wKey)
        {
            var batch = _activeWindowsStore.Get(wKey);
            if (batch == null)
                return; // nothing to remove

            _nextOperator?.Process(batch);

            if (_storeResultsForAudit && _auditStore != null)
            {
                lock (_auditStore)
                {
                    _auditStore.Put(wKey, batch);
                }
            }

            _activeWindowsStore.Remove(wKey);
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled && _telemetryProvider != null)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;
            if (_telemetryProvider != null)
            {
                var metrics = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metrics.CreateCounter(
                    $"tumbling_window_processed_{typeof(TInput).Name}",
                    "Number of items processed by TumblingWindowOperator");
                _processingTimeHistogram = metrics.CreateHistogram(
                    $"tumbling_window_processing_time_{typeof(TInput).Name}",
                    "Processing time for TumblingWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider()
                    .GetTracer($"TumblingWindowOperator_{typeof(TInput).Name}");

                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = ms => _processingTimeHistogram.Record(ms);
            }

            if (_nextOperator is ITelemetryEnabled op)
            {
                op.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IDataStore> GetStateStores()
        {
            yield return _activeWindowsStore;
            if (_storeResultsForAudit && _auditStore != null)
                yield return _auditStore;
            if (_enableCheckpointing && _checkpointStore != null)
                yield return _checkpointStore;
        }

        #region Checkpointing

        public void Checkpoint()
        {
            if (!_enableCheckpointing) return;

            var snapshot = new TumblingCheckpointState
            {
                UseEventTime = _useEventTime,
                MaxEventTime = _maxEventTime,
                CurrentProcWindowStart = _currentProcWindowStart,
                CurrentProcWindowEnd = _currentProcWindowEnd
            };
            lock (_checkpointStore)
            {
                _checkpointStore.Put(CHECKPOINT_KEY, snapshot);
            }
        }

        public void RestoreCheckpoint()
        {
            if (!_enableCheckpointing) return;

            TumblingCheckpointState snapshot;
            lock (_checkpointStore)
            {
                snapshot = _checkpointStore.Get(CHECKPOINT_KEY);
            }

            try
            {
                if (snapshot == null) return;

                lock (_syncLock)
                {
                    _maxEventTime = snapshot.MaxEventTime;
                    _currentProcWindowStart = snapshot.CurrentProcWindowStart;
                    _currentProcWindowEnd = snapshot.CurrentProcWindowEnd;
                }
            }
            catch
            {
                // no-op if parse fails
            }
        }

        #endregion

        #region Lifecycle Management

        /// <summary>
        /// Optional: Stop the background timer. If your stream calls operator.Stop() or similar,
        /// you can dispose of resources here.
        /// </summary>
        public void Stop()
        {
            lock (_syncLock)
            {
                _closingTimer?.Stop();
                _closingTimer?.Dispose();
            }
        }

        #endregion
    }
}
