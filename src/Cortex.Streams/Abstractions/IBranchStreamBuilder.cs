using Cortex.States;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Abstractions
{
    /// <summary>
    /// Provides methods to build a branch within the stream processing pipeline.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the branch.</typeparam>
    public interface IBranchStreamBuilder<TIn, TCurrent>
    {
        /// <summary>
        /// Adds a filter operator to the branch.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The branch stream builder for method chaining.</returns>
        IBranchStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);

        /// <summary>
        /// Adds a map operator to the branch to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The branch stream builder with the new data type.</returns>
        IBranchStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);

        /// <summary>
        /// Adds a FlatMap operator to the stream. For each input element, it produces zero or more output elements.
        /// </summary>
        /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
        /// <typeparam name="TNext">The type of data emitted after flat-mapping.</typeparam>
        /// <param name="flatMapFunction">A function that maps an input element to zero or more output elements.</param>
        /// <returns>A stream builder emitting elements of type TNext.</returns>
        IBranchStreamBuilder<TIn, TNext> FlatMap<TNext>(Func<TCurrent, IEnumerable<TNext>> flatMapFunction);

        /// <summary>
        /// Groups the stream data by a specified key selector.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="stateStore">An optional state store to use for storing group state.</param>
        /// <returns>A stream builder with grouped data.</returns>
        IBranchStreamBuilder<TIn, TCurrent> GroupBySilently<TKey>(
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
        IBranchStreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>> GroupBy<TKey>(
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
        IBranchStreamBuilder<TIn, TCurrent> AggregateSilently<TKey, TAggregate>(
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
        IBranchStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>> Aggregate<TKey, TAggregate>(
            Func<TCurrent, TKey> keySelector,
            Func<TAggregate, TCurrent, TAggregate> aggregateFunction,
            string stateStoreName = null,
            IDataStore<TKey, TAggregate> stateStore = null);




        /// <summary>
        /// Adds a sink function to the branch to consume data.
        /// </summary>
        /// <param name="sinkFunction">An action to consume data.</param>
        void Sink(Action<TCurrent> sinkFunction);

        /// <summary>
        /// Adds a sink operator to the branch to consume data.
        /// </summary>
        /// <param name="sinkOperator">A sink operator to consume data.</param>
        void Sink(ISinkOperator<TCurrent> sinkOperator);




    }
}
