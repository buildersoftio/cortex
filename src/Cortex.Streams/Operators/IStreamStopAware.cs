using System;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Implemented by operators that run work outside the stream's emit call path (for example on a timer)
    /// and therefore cannot let a <see cref="ErrorHandling.StreamStoppedException"/> propagate to the stream.
    /// The owning stream passes in an action that stops it.
    /// </summary>
    internal interface IStreamStopAware
    {
        void SetStopAction(Action stopStream);
    }
}
