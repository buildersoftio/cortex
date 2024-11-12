using System;
namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Represents a source operator that emits data into the stream.
    /// </summary>
    /// <typeparam name="TOutput">The type of data emitted by the source operator.</typeparam>
    public interface ISourceOperator<TOutput>
    {
        /// <summary>
        /// Starts the source operator, emitting data to the provided action.
        /// </summary>
        /// <param name="emit">An action to receive emitted data.</param>
        void Start(Action<TOutput> emit);

        /// <summary>
        /// Stops the source operator.
        /// </summary>
        void Stop();
    }
}
