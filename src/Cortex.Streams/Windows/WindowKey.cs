using System;

namespace Cortex.Streams.Windows
{
    /// <summary>
    /// Represents a key identifying a time window by its start and end timestamps.
    /// </summary>
    public struct WindowKey
    {
        public DateTimeOffset WindowStart { get; }
        public DateTimeOffset WindowEnd { get; }

        public WindowKey(DateTimeOffset windowStart, DateTimeOffset windowEnd)
        {
            WindowStart = windowStart;
            WindowEnd = windowEnd;
        }

        public override string ToString()
        {
            return $"[{WindowStart:u} - {WindowEnd:u}]";
        }
    }
}
