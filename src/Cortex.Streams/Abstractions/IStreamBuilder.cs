using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Abstractions
{
    /// <summary>
    /// Adds a map operator to the stream to transform data.
    /// </summary>
    /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
    /// <param name="mapFunction">A function to transform data.</param>
    /// <returns>The stream builder with the new data type.</returns>
    /// <remarks>
    /// Use the map operator to project each element of the stream into a new form.
    /// </remarks>
    /// <example>
    /// <code>
    /// var stream = StreamBuilder<int, int>.CreateNewStream("Example Stream")
    ///     .Map(x => x * 2)
    ///     .Sink(Console.WriteLine)
    ///     .Build();
    /// </code>
    /// </example>
    public interface IStreamBuilder<TIn, TCurrent>
    {
        /// <summary>
        /// Adds a filter operator to the stream.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The stream builder for method chaining.</returns>
        IStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);

        /// <summary>
        /// Adds a map operator to the stream to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The stream builder with the new data type.</returns>
        IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);

        /// <summary>
        /// Adds a sink function to the stream.
        /// </summary>
        /// <param name="sinkFunction">An action to consume data.</param>
        /// <returns>A sink builder to build the stream.</returns>
        ISinkBuilder<TIn, TCurrent> Sink(Action<TCurrent> sinkFunction);

        /// <summary>
        /// Adds a sink operator to the stream.
        /// </summary>
        /// <param name="sinkOperator">A sink operator to consume data.</param>
        /// <returns>A sink builder to build the stream.</returns>
        ISinkBuilder<TIn, TCurrent> Sink(ISinkOperator<TCurrent> sinkOperator);

        /// <summary>
        /// Adds a branch to the stream with a specified name and configuration.
        /// </summary>
        /// <param name="name">The name of the branch.</param>
        /// <param name="branchConfig">An action to configure the branch.</param>
        /// <returns>The stream builder for method chaining.</returns>
        IStreamBuilder<TIn, TCurrent> AddBranch(string name, Action<IBranchStreamBuilder<TIn, TCurrent>> config);

        /// <summary>
        /// Builds the stream
        /// </summary>
        /// <returns></returns>
        IStream<TIn, TCurrent> Build();

    }
}
