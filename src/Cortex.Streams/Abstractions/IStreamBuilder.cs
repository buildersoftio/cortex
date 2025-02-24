using Cortex.States;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

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
        /// Adds a FlatMap operator to the stream. For each input element, it produces zero or more output elements.
        /// </summary>
        /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
        /// <typeparam name="TNext">The type of data emitted after flat-mapping.</typeparam>
        /// <param name="flatMapFunction">A function that maps an input element to zero or more output elements.</param>
        /// <returns>A stream builder emitting elements of type TNext.</returns>
        IStreamBuilder<TIn, TNext> FlatMap<TNext>(Func<TCurrent, IEnumerable<TNext>> flatMapFunction);

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


        /// <summary>
        /// Groups the stream data by a specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="stateStore">An optional state store to use for storing group state.</param>
        /// <returns>A stream builder with grouped data.</returns>
        IStreamBuilder<TIn, TCurrent> GroupBySilently<TKey>(
            Func<TCurrent, TKey> keySelector,
            string stateStoreName = null,
            IDataStore<TKey, List<TCurrent>> stateStore = null);

        /// <summary>
        /// Groups the stream data by a specified key selector silently.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="stateStore">An optional state store to use for storing group state.</param>
        /// <returns>A stream builder with grouped data.</returns>
        IStreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>> GroupBy<TKey>(
            Func<TCurrent, TKey> keySelector,
            string stateStoreName = null,
            IDataStore<TKey, List<TCurrent>> stateStore = null);

        /// <summary>
        /// Aggregates the stream data using a specified aggregation function.
        /// </summary>
        /// <typeparam name="TAggregate">The type of the aggregate value.</typeparam>
        /// <param name="aggregateFunction">A function to aggregate data.</param>
        /// <param name="stateStore">An optional state store to use for storing aggregate state.</param>
        /// <returns>A stream builder with aggregated data.</returns>
        IStreamBuilder<TIn, TCurrent> AggregateSilently<TKey, TAggregate>(
            Func<TCurrent, TKey> keySelector,
            Func<TAggregate, TCurrent, TAggregate> aggregateFunction,
            string stateStoreName = null,
            IDataStore<TKey, TAggregate> stateStore = null);

        /// <summary>
        /// Aggregates the stream data using a specified aggregation function silently in the background.
        /// </summary>
        /// <typeparam name="TAggregate">The type of the aggregate value.</typeparam>
        /// <param name="aggregateFunction">A function to aggregate data.</param>
        /// <param name="stateStore">An optional state store to use for storing aggregate state.</param>
        /// <returns>A stream builder with input data.</returns>
        IStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>> Aggregate<TKey, TAggregate>(
            Func<TCurrent, TKey> keySelector,
            Func<TAggregate, TCurrent, TAggregate> aggregateFunction,
            string stateStoreName = null,
            IDataStore<TKey, TAggregate> stateStore = null);


        /// <summary>
        /// Joins the current stream with a state-backed table (right side) based on a shared key.
        /// For each item in the left (current) stream, a key is extracted and matched against entries
        /// in <paramref name="rightStateStore"/>. If a match is found, the two items are combined
        /// by <paramref name="joinFunction"/> to form a result of type <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TRight">The type of the elements stored in the right state store.</typeparam>
        /// <typeparam name="TKey">The type of the key used for matching left stream elements to right elements.</typeparam>
        /// <typeparam name="TResult">The type of the result produced by joining a left element with a right element.</typeparam>
        /// <param name="rightStateStore">
        /// The state store mapping keys of type <typeparamref name="TKey"/> to values of type <typeparamref name="TRight"/>.
        /// </param>
        /// <param name="keySelector">
        /// A function that extracts the key from the left (current) stream element of type <c>TCurrent</c>.
        /// </param>
        /// <param name="joinFunction">
        /// A function that combines the left element (of type <c>TCurrent</c>) and the matching right element
        /// (of type <typeparamref name="TRight"/>) to produce a result of type <typeparamref name="TResult"/>.
        /// </param>
        /// <returns>
        /// An <see cref="IStreamBuilder{TIn, TResult}"/> representing the pipeline after the join operation.
        /// </returns>
        IStreamBuilder<TIn, TResult> Join<TRight, TKey, TResult>(
            IDataStore<TKey, TRight> rightStateStore,
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, TRight, TResult> joinFunction);


        IStreamBuilder<TIn, TCurrent> SetNext(IOperator customOperator);
    }
}
