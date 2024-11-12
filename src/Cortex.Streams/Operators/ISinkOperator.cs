namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Represents a sink operator that consumes data from the stream.
    /// </summary>
    /// <typeparam name="TInput">The type of data consumed by the sink operator.</typeparam>
    public interface ISinkOperator<TInput>
    {
        /// <summary>
        /// Starts the sink operator.
        /// </summary>
        void Start();

        /// <summary>
        /// Processes the input data.
        /// </summary>
        /// <param name="input">The data to process.</param>
        void Process(TInput input);

        /// <summary>
        /// Stops the sink operator.
        /// </summary>
        void Stop();
    }
}
