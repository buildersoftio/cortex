using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Windows;
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
        IStreamBuilder<TIn, TCurrent> GroupBySilently<TKey>(
            Func<TCurrent, TKey> keySelector,
            string stateStoreName = null,
            IStateStore<TKey, List<TCurrent>> stateStore = null);

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
            IStateStore<TKey, List<TCurrent>> stateStore = null);

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
            IStateStore<TKey, TAggregate> stateStore = null);

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
            IStateStore<TKey, TAggregate> stateStore = null);


        /// <summary>
        /// Adds a tumbling window operator to the stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="windowDuration">The duration of the tumbling window.</param>
        /// <param name="windowFunction">A function to process the data in the window.</param>
        /// <param name="windowStateStoreName">Optional name for the state store.</param>
        /// <param name="windowResultsStateStoreName">Optional name for the results state store.</param>
        /// <param name="windowStateStore">Optional state store instance for window state.</param>
        /// <param name="windowResultsStateStore">Optional state store instance for window results.</param>
        /// <returns>A stream builder with the new data type.</returns>
        IStreamBuilder<TIn, TWindowOutput> TumblingWindow<TKey, TWindowOutput>(
            Func<TCurrent, TKey> keySelector,
            TimeSpan windowDuration,
            Func<IEnumerable<TCurrent>, TWindowOutput> windowFunction,
            string windowStateStoreName = null,
            string windowResultsStateStoreName = null,
            IStateStore<TKey, WindowState<TCurrent>> windowStateStore = null,
            IStateStore<WindowKey<TKey>, TWindowOutput> windowResultsStateStore = null);


        /// <summary>
        /// Adds a sliding window operator to the stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="windowDuration">The duration of the sliding window.</param>
        /// <param name="slideInterval">The interval at which the window slides.</param>
        /// <param name="windowFunction">A function to process the data in the window.</param>
        /// <param name="windowStateStoreName">Optional name for the state store.</param>
        /// <param name="windowResultsStateStoreName">Optional name for the results state store.</param>
        /// <param name="windowStateStore">Optional state store instance for window state.</param>
        /// <param name="windowResultsStateStore">Optional state store instance for window results.</param>
        /// <returns>A stream builder with the new data type.</returns>
        IStreamBuilder<TIn, TWindowOutput> SlidingWindow<TKey, TWindowOutput>(
            Func<TCurrent, TKey> keySelector,
            TimeSpan windowDuration,
            TimeSpan slideInterval,
            Func<IEnumerable<TCurrent>, TWindowOutput> windowFunction,
            string windowStateStoreName = null,
            string windowResultsStateStoreName = null,
            IStateStore<WindowKey<TKey>, List<TCurrent>> windowStateStore = null,
            IStateStore<WindowKey<TKey>, TWindowOutput> windowResultsStateStore = null);

        /// <summary>
        /// Adds a session window operator to the stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the key to group by.</typeparam>
        /// <typeparam name="TSessionOutput">The type of the output after session windowing.</typeparam>
        /// <param name="keySelector">A function to extract the key from data.</param>
        /// <param name="inactivityGap">The inactivity gap duration to define session boundaries.</param>
        /// <param name="sessionFunction">A function to process the data in the session.</param>
        /// <param name="sessionStateStoreName">Optional name for the state store.</param>
        /// <param name="sessionResultsStateStoreName">Optional name for the results state store.</param>
        /// <param name="sessionStateStore">Optional state store instance for session state.</param>
        /// <param name="sessionResultsStateStore">Optional state store instance for session results.</param>
        /// <returns>A stream builder with the new data type.</returns>
        IStreamBuilder<TIn, TSessionOutput> SessionWindow<TKey, TSessionOutput>(
            Func<TCurrent, TKey> keySelector,
            TimeSpan inactivityGap,
            Func<IEnumerable<TCurrent>, TSessionOutput> sessionFunction,
            string sessionStateStoreName = null,
            string sessionResultsStateStoreName = null,
            IStateStore<TKey, SessionState<TCurrent>> sessionStateStore = null,
            IStateStore<SessionKey<TKey>, TSessionOutput> sessionResultsStateStore = null);


        IStreamBuilder<TIn, TCurrent> SetNext(IOperator customOperator);
    }
}
