using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;
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
        /// Adds a conditional branch: only items matching <paramref name="condition"/> enter it,
        /// while the main pipeline still receives every item.
        /// </summary>
        /// <param name="name">The name of the branch.</param>
        /// <param name="condition">A predicate deciding which items enter the branch. It follows the stream's error handling.</param>
        /// <param name="config">An action to configure the branch.</param>
        /// <returns>The stream builder for method chaining.</returns>
        IStreamBuilder<TIn, TCurrent> AddBranch(string name, Func<TCurrent, bool> condition, Action<IBranchStreamBuilder<TIn, TCurrent>> config);

        /// <summary>
        /// Creates a fan-out pattern to send data to multiple sinks simultaneously.
        /// Use this when you need to send the same data to multiple destinations
        /// without intermediate transformations between sinks.
        /// </summary>
        /// <param name="config">An action to configure the fan-out sinks using the builder.</param>
        /// <returns>A fan-out builder to configure and build the stream.</returns>
        /// <remarks>
        /// <para>
        /// FanOut is simpler than AddBranch when you only need multiple sinks without
        /// per-sink transformations. For complex branching with different transformations
        /// per branch, use <see cref="AddBranch"/> instead.
        /// </para>
        /// <para>
        /// Each sink can optionally have a filter predicate to receive only matching data.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// var stream = StreamBuilder&lt;Order&gt;.CreateNewStream("OrderProcessor")
        ///     .Stream()
        ///     .Map(order => EnrichOrder(order))
        ///     .FanOut(fanOut => fanOut
        ///         .To("database", order => SaveToDatabase(order))
        ///         .To("kafka", order => PublishToKafka(order))
        ///         .To("alerts", order => order.Amount > 10000, order => SendAlert(order)))
        ///     .Build();
        /// </code>
        /// </example>
        IFanOutBuilder<TIn, TCurrent> FanOut(Action<IFanOutBuilder<TIn, TCurrent>> config);

        /// <summary>
        /// Splits the stream into branches and merges their outputs back into one stream.
        /// Each item reaches every (matching) branch in the order they were added, and every
        /// branch output continues to the operators chained after the merge.
        /// </summary>
        /// <typeparam name="TNext">The type of the merged items; every branch must produce it.</typeparam>
        /// <param name="config">An action that adds the branches with <see cref="IMergeBuilder{TIn, TCurrent, TNext}.Branch(string, Func{IBranchStreamBuilder{TIn, TCurrent}, IBranchStreamBuilder{TIn, TNext}})"/>.</param>
        /// <returns>A stream builder for the merged stream.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
        /// <exception cref="InvalidOperationException">No branch was added.</exception>
        /// <example>
        /// <code>
        /// var stream = StreamBuilder&lt;Order&gt;.CreateNewStream("Orders")
        ///     .Stream()
        ///     .Merge&lt;string&gt;(merge =&gt; merge
        ///         .Branch("all", b =&gt; b.Map(o =&gt; $"order {o.Id}"))
        ///         .Branch("large", o =&gt; o.Amount &gt; 10000, b =&gt; b.Map(o =&gt; $"large order {o.Id}")))
        ///     .Sink(Console.WriteLine)
        ///     .Build();
        /// </code>
        /// </example>
        IStreamBuilder<TIn, TNext> Merge<TNext>(Action<IMergeBuilder<TIn, TCurrent, TNext>> config);

        /// <summary>
        /// Builds the stream
        /// </summary>
        /// <returns></returns>
        IStream<TIn> Build();


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

        /// <summary>
        /// Performs a left join between the current stream and a state-backed table (right side) based on a shared key.
        /// Unlike an inner join, this operation emits a result for every left element, even when no matching right element exists.
        /// When no match is found, the join function receives <c>default(TRight)</c> for the right element.
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
        /// (or <c>default(TRight)</c> if no match) to produce a result of type <typeparamref name="TResult"/>.
        /// </param>
        /// <returns>
        /// An <see cref="IStreamBuilder{TIn, TResult}"/> representing the pipeline after the left join operation.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Use a left join when you want to enrich stream data with optional reference data that may not always exist.
        /// </para>
        /// <example>
        /// <code>
        /// var stream = StreamBuilder&lt;Order&gt;.CreateNewStream("OrderEnrichment")
        ///     .Stream()
        ///     .LeftJoin(
        ///         customerStore,
        ///         order => order.CustomerId,
        ///         (order, customer) => new EnrichedOrder(order, customer)) // customer may be null
        ///     .Sink(Console.WriteLine)
        ///     .Build();
        /// </code>
        /// </example>
        /// </remarks>
        IStreamBuilder<TIn, TResult> LeftJoin<TRight, TKey, TResult>(
                    IDataStore<TKey, TRight> rightStateStore,
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, TRight, TResult> joinFunction);

        /// <summary>
        /// Performs a windowed join between the current stream (left) and another stream (right) based on a shared key.
        /// Elements from both streams are buffered within the configured time window and matched when they share the same key.
        /// </summary>
        /// <typeparam name="TRight">The type of elements in the right stream.</typeparam>
        /// <typeparam name="TKey">The type of the key used for matching elements from both streams.</typeparam>
        /// <typeparam name="TResult">The type of the result produced by joining matched elements.</typeparam>
        /// <param name="rightStreamOperator">
        /// The operator that provides the right stream. Use <see cref="StreamStreamJoinOperator{TLeft, TRight, TKey, TResult}"/>
        /// and call its <c>ProcessRight</c> method to feed elements from the right stream.
        /// </param>
        /// <param name="leftKeySelector">Function to extract the join key from left stream elements.</param>
        /// <param name="rightKeySelector">Function to extract the join key from right stream elements.</param>
        /// <param name="leftTimestampSelector">Function to extract the event timestamp from left stream elements.</param>
        /// <param name="rightTimestampSelector">Function to extract the event timestamp from right stream elements.</param>
        /// <param name="joinFunction">Function that combines matched left and right elements to produce a result.</param>
        /// <param name="configuration">
        /// Optional configuration specifying window size, join type (inner/left/outer), and other settings.
        /// Defaults to an inner join with a 5-minute window.
        /// </param>
        /// <returns>
        /// An <see cref="IStreamBuilder{TIn, TResult}"/> representing the pipeline after the stream-stream join.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Stream-stream joins are useful for correlating events from two different streams, such as:
        /// <list type="bullet">
        ///   <item>Matching orders with their corresponding shipments</item>
        ///   <item>Correlating user clicks with page impressions</item>
        ///   <item>Joining sensor readings from different devices</item>
        /// </list>
        /// </para>
        /// <example>
        /// <code>
        /// var joinOperator = new StreamStreamJoinOperator&lt;Order, Shipment, string, OrderShipment&gt;(
        ///     order => order.OrderId,
        ///     shipment => shipment.OrderId,
        ///     order => order.Timestamp,
        ///     shipment => shipment.Timestamp,
        ///     (order, shipment) => new OrderShipment(order, shipment),
        ///     StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(30)));
        ///     
        /// var stream = StreamBuilder&lt;Order&gt;.CreateNewStream("OrderShipmentJoin")
        ///     .Stream()
        ///     .JoinStream(joinOperator)
        ///     .Sink(result => Console.WriteLine($"Order {result.Order.Id} shipped!"))
        ///     .Build();
        ///     
        /// // Feed shipments to the join operator from another source
        /// shipmentStream.Subscribe(shipment => joinOperator.ProcessRight(shipment));
        /// </code>
        /// </example>
        /// </remarks>
        IStreamBuilder<TIn, TResult> JoinStream<TRight, TKey, TResult>(
            StreamStreamJoinOperator<TCurrent, TRight, TKey, TResult> joinOperator);


                /// <summary>
                /// Applies a tumbling window to the stream. Tumbling windows are fixed-size, non-overlapping windows.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="windowSize">The size of each tumbling window.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing window data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> TumblingWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan windowSize,
                    string stateStoreName = null,
                    IDataStore<string, List<TCurrent>> stateStore = null);

                /// <summary>
                /// Applies a sliding window to the stream. Sliding windows have a fixed size but overlap based on the slide interval.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="windowSize">The size of each sliding window.</param>
                /// <param name="slideInterval">The interval at which the window slides.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing window data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> SlidingWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan windowSize,
                    TimeSpan slideInterval,
                    string stateStoreName = null,
                    IDataStore<string, List<TCurrent>> stateStore = null);

                /// <summary>
                /// Applies a session window to the stream. Session windows group events by activity sessions separated by inactivity gaps.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition sessions.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="inactivityGap">The duration of inactivity after which a session is closed.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing session data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> SessionWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan inactivityGap,
                    string stateStoreName = null,
                    IDataStore<string, SessionState<TCurrent>> stateStore = null);

                /// <summary>
                /// Applies an advanced tumbling window with custom triggers and state modes.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="windowSize">The size of each tumbling window.</param>
                /// <param name="config">The window configuration with trigger and state mode settings.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing window data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedTumblingWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan windowSize,
                    WindowConfiguration<TCurrent> config,
                    string stateStoreName = null,
                    IDataStore<string, List<TCurrent>> stateStore = null);

                /// <summary>
                /// Applies an advanced sliding window with custom triggers and state modes.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="windowSize">The size of each sliding window.</param>
                /// <param name="slideInterval">The interval at which the window slides.</param>
                /// <param name="config">The window configuration with trigger and state mode settings.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing window data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedSlidingWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan windowSize,
                    TimeSpan slideInterval,
                    WindowConfiguration<TCurrent> config,
                    string stateStoreName = null,
                    IDataStore<string, List<TCurrent>> stateStore = null);

                /// <summary>
                /// Applies an advanced session window with custom triggers and state modes.
                /// </summary>
                /// <typeparam name="TKey">The type of the key used to partition sessions.</typeparam>
                /// <param name="keySelector">A function to extract the key from each input item.</param>
                /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                /// <param name="inactivityGap">The duration of inactivity after which a session is closed.</param>
                /// <param name="config">The window configuration with trigger and state mode settings.</param>
                /// <param name="stateStoreName">Optional name for the state store.</param>
                /// <param name="stateStore">Optional state store to use for storing session data.</param>
                /// <returns>A stream builder emitting window results.</returns>
                IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedSessionWindow<TKey>(
                    Func<TCurrent, TKey> keySelector,
                    Func<TCurrent, DateTime> timestampSelector,
                    TimeSpan inactivityGap,
                    WindowConfiguration<TCurrent> config,
                    string stateStoreName = null,
                    IDataStore<string, AdvancedSessionState<TCurrent>> stateStore = null);


                IStreamBuilder<TIn, TCurrent> SetNext(IOperator customOperator);

                /// <summary>
                /// Switches the stream to event time from this point on. Items pass through unchanged; the watermark
                /// (the newest event timestamp minus the strategy's out-of-orderness bound) is tracked here, and every
                /// window and stream-stream join downstream closes its windows by watermark instead of the wall clock.
                /// Operators before this call keep processing time. Call it once per path, before the first window or join.
                /// </summary>
                /// <param name="timestampSelector">Extracts the event timestamp (UTC) from an item.</param>
                /// <param name="strategy">The out-of-orderness bound, allowed lateness, late-event handler, idle timeout and optional state store.</param>
                /// <returns>The stream builder for method chaining.</returns>
                /// <exception cref="ArgumentNullException"><paramref name="timestampSelector"/> or <paramref name="strategy"/> is null.</exception>
                /// <example>
                /// <code>
                /// var stream = StreamBuilder&lt;Reading&gt;.CreateNewStream("Sensors")
                ///     .Stream()
                ///     .WithWatermarks(r =&gt; r.MeasuredAt, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5)))
                ///     .TumblingWindow(r =&gt; r.SensorId, r =&gt; r.MeasuredAt, TimeSpan.FromMinutes(1))
                ///     .Sink(window =&gt; Console.WriteLine(window.Items.Count))
                ///     .Build();
                /// </code>
                /// </example>
                IStreamBuilder<TIn, TCurrent> WithWatermarks(Func<TCurrent, DateTime> timestampSelector, WatermarkStrategy strategy);
            }
        }
