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
        //IStreamBuilder<TIn, KeyValuePair<TKey, TCurrent>> GroupBy<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, IStateStore<TKey,List<TCurrent>> stateStore = null);
        IStreamBuilder<TIn, TCurrent> GroupBy<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, IStateStore<TKey, List<TCurrent>> stateStore = null);

        /// <summary>
        /// Aggregates the stream data using a specified aggregation function.
        /// </summary>
        /// <typeparam name="TAggregate">The type of the aggregate value.</typeparam>
        /// <param name="aggregateFunction">A function to aggregate data.</param>
        /// <param name="stateStore">An optional state store to use for storing aggregate state.</param>
        /// <returns>A stream builder with aggregated data.</returns>
        IStreamBuilder<TIn, TCurrent> Aggregate<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, IStateStore<TKey, TAggregate> stateStore = null);
        //IStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>> Aggregate<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, IStateStore<TKey, TAggregate> stateStore = null);



        /// <summary>
        /// Adds a tumbling window operator to the stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="windowDuration">The duration of the tumbling window.</param>
        /// <param name="windowFunction">A function to process the data in the window.</param>
        /// <param name="stateStoreName">Optional name for the state store.</param>
        /// <param name="stateStore">Optional state store instance.</param>
        /// <returns>A stream builder with the new data type.</returns>
        IStreamBuilder<TIn, TWindowOutput> TumblingWindow<TKey, TWindowOutput>(
            Func<TCurrent, TKey> keySelector,
            TimeSpan windowDuration,
            Func<IEnumerable<TCurrent>, TWindowOutput> windowFunction,
            string stateStoreName = null,
            string windowResultsStateStoreName = null,
            IStateStore<TKey, List<TCurrent>> stateStore = null,
            IStateStore<(TKey, DateTime), TWindowOutput> windowResultsStateStore = null);


        /// <summary>
        /// Adds Sliding window operator to the stream
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="windowSize"></param>
        /// <param name="advanceBy"></param>
        /// <param name="windowFunction"></param>
        /// <param name="windowStateStoreName"></param>
        /// <param name="windowResultsStateStoreName"></param>
        /// <param name="windowStateStore"></param>
        /// <param name="windowResultsStateStore"></param>
        /// <returns></returns>
        IStreamBuilder<TIn, TWindowOutput> SlidingWindow<TKey, TWindowOutput>(
            Func<TCurrent, TKey> keySelector,
            TimeSpan windowSize,
            TimeSpan advanceBy,
            Func<IEnumerable<TCurrent>, TWindowOutput> windowFunction,
            string windowStateStoreName = null,
            string windowResultsStateStoreName = null,
            IStateStore<TKey, List<(TCurrent, DateTime)>> windowStateStore = null,
            IStateStore<(TKey, DateTime), TWindowOutput> windowResultsStateStore = null);
    }
}
