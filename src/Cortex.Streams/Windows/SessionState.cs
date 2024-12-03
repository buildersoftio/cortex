using System;
using System.Collections.Generic;

namespace Cortex.Streams.Windows
{
    /// <summary>
    /// Represents the state of a session window for a specific key.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    public class SessionState<TInput>
    {
        public DateTime SessionStartTime { get; set; }
        public DateTime LastEventTime { get; set; }
        public List<TInput> Events { get; set; }
    }
}
