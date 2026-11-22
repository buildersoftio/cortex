using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.ErrorHandling;
using Cortex.Streams.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cortex.Streams.Watermarks
{
    /// <summary>
    /// The part of a watermark operator the owning stream drives: wiring, pause and resume.
    /// </summary>
    internal interface IWatermarkGenerator
    {
        EventTimeContext Context { get; }
        DateTime CurrentWatermark { get; }
        void SetSubscribers(IWatermarkAware[] subscribers);
        void Pause();
        void Resume();
    }

    /// <summary>
    /// Pass-through operator that assigns watermarks: it forwards every item unchanged, tracks the newest
    /// event timestamp, and tells every <see cref="IWatermarkAware"/> operator downstream when the watermark advances.
    /// Added by <c>WithWatermarks</c>; it can also be added with <c>SetNext</c>.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    public class WatermarkOperator<T> : IOperator, IHasNextOperators, ITelemetryEnabled, IErrorHandlingEnabled,
        IStreamStopAware, IWatermarkGenerator, IStatefulOperator, IDisposable
    {
        private const long NoTimestamp = -1;
        private static readonly IWatermarkAware[] NoSubscribers = new IWatermarkAware[0];
        private static readonly long MinIdleCheckTicks = TimeSpan.FromMilliseconds(10).Ticks;
        private static readonly long MaxIdleCheckTicks = TimeSpan.FromSeconds(1).Ticks;
        private static readonly long SaveIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

        private readonly Func<T, DateTime> _timestampSelector;
        private readonly long _delayTicks;
        private readonly long _idleTimeoutTicks;
        private readonly Func<DateTime> _clock;
        private readonly object _notifyLock = new object();
        private readonly object _timerLock = new object();

        private IOperator _nextOperator;
        private IWatermarkAware[] _subscribers = NoSubscribers;
        private long _maxTimestampTicks = NoTimestamp;
        private long _watermarkTicks;
        private long _notifiedTicks;
        private long _lastEventAtTicks;
        private Timer _idleTimer;
        private bool _paused;
        private bool _disposed;
        private Action _stopStream;

        // Persistence, guarded by _notifyLock
        private readonly IDataStore<string, long> _stateStore;
        private long _savedWatermarkTicks;
        private long _lastSaveAtTicks;

        /// <summary>
        /// Creates a watermark operator.
        /// </summary>
        /// <param name="timestampSelector">Extracts the event timestamp (UTC) from an item.</param>
        /// <param name="strategy">How far events may be out of order, lateness and idleness settings.</param>
        public WatermarkOperator(Func<T, DateTime> timestampSelector, WatermarkStrategy strategy)
        {
            _timestampSelector = timestampSelector ?? throw new ArgumentNullException(nameof(timestampSelector));
            Strategy = strategy ?? throw new ArgumentNullException(nameof(strategy));
            Context = new EventTimeContext(strategy);

            _delayTicks = strategy.MaxOutOfOrderness.Ticks;
            _idleTimeoutTicks = strategy.IdleTimeout?.Ticks ?? 0;
            _clock = strategy.Clock;

            _stateStore = strategy.StateStore;
            if (_stateStore != null)
            {
                Restore();
            }
        }

        // Resumes where a previous run stopped; operators downstream get the restored watermark when they are wired
        private void Restore()
        {
            if (_stateStore.ContainsKey(WatermarkStrategy.MaxTimestampStateKey))
            {
                _maxTimestampTicks = _stateStore.Get(WatermarkStrategy.MaxTimestampStateKey);
            }

            if (_stateStore.ContainsKey(WatermarkStrategy.WatermarkStateKey))
            {
                _watermarkTicks = _stateStore.Get(WatermarkStrategy.WatermarkStateKey);
                _notifiedTicks = _watermarkTicks;
                _savedWatermarkTicks = _watermarkTicks;
            }
        }

        /// <summary>The strategy this operator applies.</summary>
        public WatermarkStrategy Strategy { get; }

        /// <summary>The context shared with every event-time operator downstream.</summary>
        public EventTimeContext Context { get; }

        /// <summary>The current watermark; <see cref="DateTime.MinValue"/> before the first event.</summary>
        public DateTime CurrentWatermark => new DateTime(Volatile.Read(ref _watermarkTicks), DateTimeKind.Utc);

        /// <summary>The number of events dropped as late by the operators downstream.</summary>
        public long LateEventCount => Context.LateEventCount;

        public void Process(object input)
        {
            if (!(input is T item))
            {
                _nextOperator?.Process(input);
                return;
            }

            var timestampTicks = _timestampSelector(item).Ticks;

            // The item first, so it reaches every operator before the watermark it advances
            _nextOperator?.Process(input);

            Observe(timestampTicks);
        }

        /// <summary>
        /// Advances the watermark if the source has been idle longer than <see cref="WatermarkStrategy.IdleTimeout"/>:
        /// from then on it moves at wall-clock speed from where it stopped. Called by the idle timer; callable directly.
        /// </summary>
        public void CheckIdle()
        {
            if (_idleTimeoutTicks == 0)
                return;

            var maxTimestamp = Volatile.Read(ref _maxTimestampTicks);
            var lastEventAt = Volatile.Read(ref _lastEventAtTicks);

            // Idle only counts after an event of this run
            if (maxTimestamp == NoTimestamp || lastEventAt == 0)
                return;

            var idleFor = _clock().Ticks - lastEventAt - _idleTimeoutTicks;
            if (idleFor <= 0)
                return;

            AdvanceTo(maxTimestamp - _delayTicks + idleFor);
        }

        private void Observe(long timestampTicks)
        {
            if (_idleTimeoutTicks != 0)
            {
                Volatile.Write(ref _lastEventAtTicks, _clock().Ticks);
                EnsureIdleTimer();
            }

            var current = Volatile.Read(ref _maxTimestampTicks);
            while (timestampTicks > current)
            {
                var observed = Interlocked.CompareExchange(ref _maxTimestampTicks, timestampTicks, current);
                if (observed == current)
                {
                    AdvanceTo(timestampTicks - _delayTicks);
                    return;
                }
                current = observed;
            }
        }

        private void AdvanceTo(long candidateTicks)
        {
            if (candidateTicks > DateTime.MaxValue.Ticks)
                candidateTicks = DateTime.MaxValue.Ticks;

            var current = Volatile.Read(ref _watermarkTicks);
            while (candidateTicks > current)
            {
                var observed = Interlocked.CompareExchange(ref _watermarkTicks, candidateTicks, current);
                if (observed == current)
                {
                    Notify();
                    return;
                }
                current = observed;
            }
        }

        // One notification at a time, in increasing order; a slower thread that lost the race sends nothing
        private void Notify()
        {
            lock (_notifyLock)
            {
                var watermarkTicks = Volatile.Read(ref _watermarkTicks);
                if (watermarkTicks <= _notifiedTicks)
                    return;

                _notifiedTicks = watermarkTicks;
                var watermark = new DateTime(watermarkTicks, DateTimeKind.Utc);
                foreach (var subscriber in Volatile.Read(ref _subscribers))
                {
                    subscriber.OnWatermark(watermark);
                }

                Save(force: false);
            }
        }

        // Called under _notifyLock: writes the watermark when it changed, at most once per second unless forced
        private void Save(bool force)
        {
            if (_stateStore == null)
                return;

            var watermarkTicks = Volatile.Read(ref _watermarkTicks);
            if (watermarkTicks == _savedWatermarkTicks)
                return;

            var now = _clock().Ticks;
            if (!force && now - _lastSaveAtTicks < SaveIntervalTicks)
                return;

            _stateStore.Put(WatermarkStrategy.MaxTimestampStateKey, Volatile.Read(ref _maxTimestampTicks));
            _stateStore.Put(WatermarkStrategy.WatermarkStateKey, watermarkTicks);
            _savedWatermarkTicks = watermarkTicks;
            _lastSaveAtTicks = now;
        }

        private void Flush()
        {
            lock (_notifyLock)
            {
                Save(force: true);
            }
        }

        public IEnumerable<IDataStore> GetStateStores()
        {
            if (_stateStore != null)
                yield return _stateStore;
        }

        private void EnsureIdleTimer()
        {
            if (Volatile.Read(ref _idleTimer) != null)
                return;

            lock (_timerLock)
            {
                if (_idleTimer != null || _disposed)
                    return;

                var period = _paused ? Timeout.InfiniteTimeSpan : IdleCheckPeriod();
                Volatile.Write(ref _idleTimer, new Timer(OnIdleTimer, null, period, period));
            }
        }

        private TimeSpan IdleCheckPeriod()
        {
            var ticks = Math.Min(Math.Max(_idleTimeoutTicks / 4, MinIdleCheckTicks), MaxIdleCheckTicks);
            return TimeSpan.FromTicks(ticks);
        }

        /// <summary>
        /// Timer callback. Nothing may escape it: an unhandled exception on a timer thread terminates the process.
        /// </summary>
        private void OnIdleTimer(object state)
        {
            try
            {
                CheckIdle();
            }
            catch (StreamStoppedException)
            {
                _stopStream?.Invoke();
            }
            catch (Exception ex)
            {
                Trace.TraceError($"WatermarkOperator<{typeof(T).Name}>: idle watermark failed: {ex}");
            }
        }

        void IWatermarkGenerator.SetSubscribers(IWatermarkAware[] subscribers)
        {
            Volatile.Write(ref _subscribers, subscribers ?? NoSubscribers);
        }

        void IWatermarkGenerator.Pause()
        {
            Flush();

            lock (_timerLock)
            {
                _paused = true;
                _idleTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }

        void IWatermarkGenerator.Resume()
        {
            lock (_timerLock)
            {
                if (!_paused || _disposed)
                    return;

                _paused = false;

                // Time spent stopped is not idle time
                Volatile.Write(ref _lastEventAtTicks, _clock().Ticks);

                var period = IdleCheckPeriod();
                _idleTimer?.Change(period, period);
            }
        }

        void IStreamStopAware.SetStopAction(Action stopStream)
        {
            _stopStream = stopStream;
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled)
            {
                nextTelemetryEnabled.SetTelemetryProvider(telemetryProvider);
            }
        }

        public void SetErrorHandling(StreamExecutionOptions options)
        {
            if (_nextOperator is IErrorHandlingEnabled nextWithErrorHandling)
            {
                nextWithErrorHandling.SetErrorHandling(options);
            }
        }

        public void Dispose()
        {
            Flush();

            lock (_timerLock)
            {
                if (_disposed)
                    return;

                _disposed = true;
                _idleTimer?.Dispose();
                _idleTimer = null;
            }
        }
    }
}
