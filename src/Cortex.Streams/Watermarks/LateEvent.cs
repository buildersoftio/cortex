using System;

namespace Cortex.Streams.Watermarks
{
    /// <summary>
    /// An event dropped because it arrived after the watermark had passed its window's end plus the allowed lateness.
    /// </summary>
    public sealed class LateEvent
    {
        public LateEvent(object item, DateTime timestamp, DateTime watermark, string operatorName)
        {
            Item = item;
            Timestamp = timestamp;
            Watermark = watermark;
            OperatorName = operatorName;
        }

        /// <summary>The dropped item, as the operator received it.</summary>
        public object Item { get; }

        /// <summary>The item's event timestamp.</summary>
        public DateTime Timestamp { get; }

        /// <summary>The operator's watermark when the item arrived.</summary>
        public DateTime Watermark { get; }

        /// <summary>The operator that dropped the item.</summary>
        public string OperatorName { get; }
    }
}
