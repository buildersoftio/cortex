using System;

namespace Cortex.Streams.Windows
{
    public class TumblingCheckpointState
    {
        public bool UseEventTime { get; set; }
        public DateTimeOffset MaxEventTime { get; set; }
        public DateTimeOffset CurrentProcWindowStart { get; set; }
        public DateTimeOffset CurrentProcWindowEnd { get; set; }
    }
}
