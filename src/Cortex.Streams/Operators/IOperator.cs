namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Represents a generic operator in the stream processing pipeline.
    /// </summary>
    public interface IOperator
    {
        /// <summary>
        /// Processes the input object and passes it to the next operator in the pipeline.
        /// </summary>
        /// <param name="input">The input object to process.</param>
        void Process(object input);

        /// <summary>
        /// Sets the next operator in the pipeline.
        /// </summary>
        /// <param name="nextOperator">The next operator to set.</param>
        void SetNext(IOperator nextOperator);
    }
}
