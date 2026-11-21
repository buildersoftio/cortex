using System;

namespace Cortex.Streams.Watermarks
{
    /// <summary>
    /// Implemented by operators that work in event time once the stream assigns watermarks
    /// (windows, stream-stream joins, or custom operators).
    /// </summary>
    /// <remarks>
    /// When a stream contains <c>WithWatermarks</c>, every operator downstream of it that implements this interface
    /// gets <see cref="EnableEventTime"/> once, before it receives a watermark, and then <see cref="OnWatermark"/>
    /// each time the watermark advances. Operators are notified in pipeline order, after the item that advanced
    /// the watermark has passed through them, and never concurrently. Operators without watermarks upstream
    /// are never called and keep their processing-time behaviour.
    /// </remarks>
    public interface IWatermarkAware
    {
        /// <summary>Switches the operator to event time.</summary>
        void EnableEventTime(EventTimeContext context);

        /// <summary>Tells the operator the watermark advanced to <paramref name="watermark"/>.</summary>
        void OnWatermark(DateTime watermark);
    }
}
