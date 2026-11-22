using Cortex.States;
using System;

namespace Cortex.Streams.Watermarks
{
    /// <summary>
    /// Describes how a stream measures event time: how far events may arrive out of order,
    /// how long late events are still accepted, what happens to events that arrive later than that,
    /// and how the watermark moves when the source goes quiet.
    /// </summary>
    /// <remarks>
    /// The watermark is <c>max event timestamp seen - MaxOutOfOrderness</c>: every event with an earlier
    /// timestamp is assumed to have arrived. A window <c>[start, end)</c> fires once the watermark reaches
    /// <c>end</c> and keeps its state until the watermark reaches <c>end + AllowedLateness</c>.
    /// Strategies are immutable: every <c>With...</c> method returns a new instance.
    /// </remarks>
    public sealed class WatermarkStrategy
    {
        /// <summary>The longest allowed lateness: the whole <see cref="DateTime"/> range. Longer values are capped to it.</summary>
        public static readonly TimeSpan MaxLateness = TimeSpan.FromTicks(DateTime.MaxValue.Ticks);

        private WatermarkStrategy(TimeSpan maxOutOfOrderness)
        {
            MaxOutOfOrderness = maxOutOfOrderness;
        }

        /// <summary>How far behind the newest event timestamp the watermark trails.</summary>
        public TimeSpan MaxOutOfOrderness { get; }

        /// <summary>
        /// After this long without events the watermark keeps advancing at wall-clock speed from where it stopped,
        /// so open windows still close. <c>null</c> (the default) means the watermark only moves with events.
        /// </summary>
        public TimeSpan? IdleTimeout { get; private set; }

        /// <summary>How long after a window's end it still accepts late events and fires again. Default: zero.</summary>
        public TimeSpan AllowedLateness { get; private set; }

        /// <summary>Called for every event dropped because it arrived after its window's lateness ended. Optional.</summary>
        public Action<LateEvent> LateEventHandler { get; private set; }

        /// <summary>
        /// Key under which the watermark (UTC ticks) is saved in <see cref="StateStore"/>.
        /// </summary>
        public const string WatermarkStateKey = "watermark";

        /// <summary>
        /// Key under which the newest event timestamp (UTC ticks) is saved in <see cref="StateStore"/>.
        /// </summary>
        public const string MaxTimestampStateKey = "max-timestamp";

        /// <summary>
        /// Optional store the watermark is saved to (at most once per second while it advances, and when the stream stops)
        /// and restored from when the stream is built. Use one store, or one store name, per <c>WithWatermarks</c>.
        /// </summary>
        public IDataStore<string, long> StateStore { get; private set; }

        /// <summary>The wall clock used for idle detection and saving. Default: <see cref="DateTime.UtcNow"/>.</summary>
        public Func<DateTime> Clock { get; private set; } = () => DateTime.UtcNow;

        /// <summary>
        /// Creates a strategy for events that arrive at most <paramref name="maxOutOfOrderness"/> out of order.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxOutOfOrderness"/> is negative.</exception>
        public static WatermarkStrategy BoundedOutOfOrderness(TimeSpan maxOutOfOrderness)
        {
            if (maxOutOfOrderness < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(maxOutOfOrderness), "The out-of-orderness bound cannot be negative.");

            return new WatermarkStrategy(maxOutOfOrderness);
        }

        /// <summary>
        /// Creates a strategy for events whose timestamps never decrease: the watermark equals the newest timestamp.
        /// </summary>
        public static WatermarkStrategy ForMonotonousTimestamps()
        {
            return new WatermarkStrategy(TimeSpan.Zero);
        }

        /// <summary>
        /// Keeps the watermark moving when no events arrive for <paramref name="idleTimeout"/>:
        /// from then on it advances at wall-clock speed from where it stopped.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="idleTimeout"/> is not positive.</exception>
        public WatermarkStrategy WithIdleTimeout(TimeSpan idleTimeout)
        {
            if (idleTimeout <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(idleTimeout), "The idle timeout must be positive.");

            var copy = Clone();
            copy.IdleTimeout = idleTimeout;
            return copy;
        }

        /// <summary>
        /// Accepts events up to <paramref name="allowedLateness"/> after their window's end; the window fires again with them.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="allowedLateness"/> is negative.</exception>
        public WatermarkStrategy WithAllowedLateness(TimeSpan allowedLateness)
        {
            if (allowedLateness < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(allowedLateness), "The allowed lateness cannot be negative.");

            // Longer than the whole DateTime range means "never": cap it so window end + lateness cannot overflow
            var copy = Clone();
            copy.AllowedLateness = allowedLateness > MaxLateness ? MaxLateness : allowedLateness;
            return copy;
        }

        /// <summary>
        /// Calls <paramref name="handler"/> for every event dropped as late. An exception it throws is traced and ignored.
        /// </summary>
        public WatermarkStrategy OnLateEvent(Action<LateEvent> handler)
        {
            var copy = Clone();
            copy.LateEventHandler = handler ?? throw new ArgumentNullException(nameof(handler));
            return copy;
        }

        /// <summary>
        /// Saves the watermark to <paramref name="stateStore"/> while it advances and restores it when the stream is built again.
        /// </summary>
        public WatermarkStrategy WithStateStore(IDataStore<string, long> stateStore)
        {
            var copy = Clone();
            copy.StateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
            return copy;
        }

        /// <summary>
        /// Replaces the wall clock used for idle detection and saving, for example with a test clock.
        /// The function must return UTC times.
        /// </summary>
        public WatermarkStrategy WithClock(Func<DateTime> utcNow)
        {
            var copy = Clone();
            copy.Clock = utcNow ?? throw new ArgumentNullException(nameof(utcNow));
            return copy;
        }

        private WatermarkStrategy Clone()
        {
            return new WatermarkStrategy(MaxOutOfOrderness)
            {
                IdleTimeout = IdleTimeout,
                AllowedLateness = AllowedLateness,
                LateEventHandler = LateEventHandler,
                StateStore = StateStore,
                Clock = Clock
            };
        }
    }
}
