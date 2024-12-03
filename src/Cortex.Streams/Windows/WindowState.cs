using System;
using System.Collections.Generic;

namespace Cortex.Streams.Windows
{
    /// <summary>
    /// Represents the state of a window for a specific key.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    public class WindowState<TInput>
    {
        public DateTime WindowStartTime { get; set; }
        public List<TInput> Events { get; set; }
    }
}
