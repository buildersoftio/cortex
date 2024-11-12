namespace Cortex.Streams.Abstractions
{
    /// <summary>
    /// Provides a method to build the stream after adding a sink.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
    public interface ISinkBuilder<TIn, TCurrent>
    {
        /// <summary>
        /// Builds the stream and returns a stream instance that can be started and stopped.
        /// </summary>
        /// <returns>A stream instance.</returns>
        IStream<TIn, TCurrent> Build();
    }
}
