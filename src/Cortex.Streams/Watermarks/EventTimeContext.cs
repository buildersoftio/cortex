using System;
using System.Diagnostics;
using System.Threading;

namespace Cortex.Streams.Watermarks
{
    /// <summary>
    /// What an event-time operator needs from the stream's <see cref="WatermarkStrategy"/>:
    /// the allowed lateness and a way to report late events.
    /// </summary>
    public sealed class EventTimeContext
    {
        private long _lateEventCount;
        private readonly Func<long> _currentWatermarkTicks;

        public EventTimeContext(WatermarkStrategy strategy)
            : this(strategy, () => DateTime.MinValue.Ticks)
        {
        }

        internal EventTimeContext(WatermarkStrategy strategy, Func<long> currentWatermarkTicks)
        {
            Strategy = strategy ?? throw new ArgumentNullException(nameof(strategy));
            _currentWatermarkTicks = currentWatermarkTicks ?? throw new ArgumentNullException(nameof(currentWatermarkTicks));
        }

        /// <summary>
        /// The stream's current watermark. It can be ahead of the last <see cref="IWatermarkAware.OnWatermark"/> call,
        /// because watermarks that no window needs are not sent.
        /// </summary>
        public DateTime CurrentWatermark => new DateTime(CurrentWatermarkTicks, DateTimeKind.Utc);

        internal long CurrentWatermarkTicks => _currentWatermarkTicks();

        /// <summary>The stream's watermark strategy.</summary>
        public WatermarkStrategy Strategy { get; }

        /// <summary>The strategy's allowed lateness.</summary>
        public TimeSpan AllowedLateness => Strategy.AllowedLateness;

        /// <summary>The number of late events reported so far.</summary>
        public long LateEventCount => Interlocked.Read(ref _lateEventCount);

        /// <summary>
        /// Counts a dropped late event and passes it to the strategy's <see cref="WatermarkStrategy.LateEventHandler"/>.
        /// An exception from the handler is traced and ignored, so it cannot break processing.
        /// </summary>
        public void ReportLateEvent(object item, DateTime timestamp, DateTime watermark, string operatorName)
        {
            CountLateEvent();

            var handler = Strategy.LateEventHandler;
            if (handler == null)
                return;

            try
            {
                handler(new LateEvent(item, timestamp, watermark, operatorName));
            }
            catch (Exception ex)
            {
                Trace.TraceError($"{operatorName}: the late-event handler threw: {ex}");
            }
        }

        /// <summary>Counts a late event that the operator reported through its own callback.</summary>
        internal void CountLateEvent()
        {
            Interlocked.Increment(ref _lateEventCount);
        }
    }
}
