using Cortex.States;
using Cortex.Streams.Abstractions;
using Cortex.Streams.ErrorHandling;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Performance;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    /// <summary>
    /// Entry point for creating a stream processing pipeline.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    public static class StreamBuilder<TIn>
    {
        /// <summary>
        /// Creates a new stream with the specified name.
        /// </summary>
        /// <param name="name">The name of the stream.</param>
        /// <returns>An initial stream builder.</returns>
        public static IInitialStreamBuilder<TIn> CreateNewStream(string name)
        {
            return new StreamBuilder<TIn, TIn>(name);
        }
    }

    /// <summary>
    /// Builds a stream processing pipeline with optional branches.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
    internal class StreamBuilder<TIn, TCurrent> : IInitialStreamBuilder<TIn>, IStreamBuilder<TIn, TCurrent>
    {
        private readonly string _name;
        private IOperator _firstOperator;
        private IOperator _lastOperator;
        private bool _sourceAdded = false;
        private readonly List<BranchOperator<TCurrent>> _branchOperators = new List<BranchOperator<TCurrent>>();
        private ForkOperator<TCurrent> _forkOperator;

        private ITelemetryProvider _telemetryProvider;
        private StreamExecutionOptions _executionOptions = StreamExecutionOptions.Default;
        private StreamPerformanceOptions _performanceOptions = StreamPerformanceOptions.Default;




        internal StreamBuilder(string name)
        {
            _name = name;
        }

        internal StreamBuilder(string name, IOperator firstOperator, IOperator lastOperator, bool sourceAdded, ITelemetryProvider telemetryProvider = null, StreamExecutionOptions executionOptions = null, StreamPerformanceOptions performanceOptions = null)
        {
            _name = name;
            _firstOperator = firstOperator;
            _lastOperator = lastOperator;
            _sourceAdded = sourceAdded;
            _telemetryProvider = telemetryProvider;
            _executionOptions = executionOptions ?? StreamExecutionOptions.Default;
            _performanceOptions = performanceOptions ?? StreamPerformanceOptions.Default;
        }

        /// <summary>
        /// Creates a new stream with the specified name.
        /// </summary>
        /// <param name="name">The name of the stream.</param>
        /// <param name="firstOperator">The first operator in the pipeline</param>
        /// <param name="lastOperator">The last operator in the pipeline</param>
        /// <returns>An initial stream builder.</returns>
        internal static IStreamBuilder<TIn, TCurrent> CreateNewStream(string name, IOperator firstOperator, IOperator lastOperator)
        {
            return new StreamBuilder<TIn, TCurrent>(name, firstOperator, lastOperator, false, null);
        }

        /// <summary>
        /// Adds a map operator to the branch to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The branch stream builder with the new data type.</returns>
        public IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction)
        {
            var mapOperator = new MapOperator<TCurrent, TNext>(mapFunction);

            if (_firstOperator == null)
            {
                _firstOperator = mapOperator;
                _lastOperator = mapOperator;
            }
            else
            {
                _lastOperator.SetNext(mapOperator);
                _lastOperator = mapOperator;
            }

            return new StreamBuilder<TIn, TNext>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        /// <summary>
        /// Adds a filter operator to the branch.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The branch stream builder for method chaining.</returns>
        public IStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate)
        {
            var filterOperator = new FilterOperator<TCurrent>(predicate);

            if (_firstOperator == null)
            {
                _firstOperator = filterOperator;
                _lastOperator = filterOperator;
            }
            else
            {
                _lastOperator.SetNext(filterOperator);
                _lastOperator = filterOperator;
            }

            return this; // Returns the current builder for method chaining
        }

        /// <summary>
        /// Adds a sink function to the branch to consume data.
        /// </summary>
        /// <param name="sinkFunction">An action to consume data.</param>
        public ISinkBuilder<TIn, TCurrent> Sink(Action<TCurrent> sinkFunction)
        {
            var sinkOperator = new SinkOperator<TCurrent>(sinkFunction);

            if (_firstOperator == null)
            {
                _firstOperator = sinkOperator;
                _lastOperator = sinkOperator;
            }
            else
            {
                _lastOperator.SetNext(sinkOperator);
                _lastOperator = sinkOperator;
            }

            return new SinkBuilder<TIn, TCurrent>(_name, _firstOperator, _branchOperators, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        /// <summary>
        /// Adds a sink operator to the branch to consume data.
        /// </summary>
        /// <param name="sinkOperator">A sink operator to consume data.</param>
        public ISinkBuilder<TIn, TCurrent> Sink(ISinkOperator<TCurrent> sinkOperator)
        {
            var sinkAdapter = new SinkOperatorAdapter<TCurrent>(sinkOperator);

            if (_firstOperator == null)
            {
                _firstOperator = sinkAdapter;
                _lastOperator = sinkAdapter;
            }
            else
            {
                _lastOperator.SetNext(sinkAdapter);
                _lastOperator = sinkAdapter;
            }

            return new SinkBuilder<TIn, TCurrent>(_name, _firstOperator, _branchOperators, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        /// <summary>
        /// Start configuring the Stream
        /// </summary>
        /// <param name="sourceOperator">Type of the Source Operator</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        IStreamBuilder<TIn, TIn> IInitialStreamBuilder<TIn>.Stream(ISourceOperator<TIn> sourceOperator)
        {
            if (_sourceAdded)
            {
                throw new InvalidOperationException("Source operator already added.");
            }

            var sourceAdapter = new SourceOperatorAdapter<TIn>(sourceOperator);

            if (_firstOperator == null)
            {
                _firstOperator = sourceAdapter;
                _lastOperator = sourceAdapter;
            }
            else
            {
                throw new InvalidOperationException("Cannot add a source operator after other operators.");
            }

            _sourceAdded = true;
            return (IStreamBuilder<TIn, TIn>)(object)this;
        }

        /// <summary>
        /// Start the stream inside the application, in-app streaming
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        IStreamBuilder<TIn, TIn> IInitialStreamBuilder<TIn>.Stream()
        {
            // In memory source added.
            if (_sourceAdded)
            {
                throw new InvalidOperationException("Source operator already added.");
            }

            _sourceAdded = true;
            return (IStreamBuilder<TIn, TIn>)(object)this;
        }


        public IStream<TIn> Build()
        {
            //return new Stream<TIn, TCurrent>(_name, _firstOperator, _branchOperators);
            return new Stream<TIn, TCurrent>(_name, _firstOperator, _branchOperators, _telemetryProvider, _executionOptions, _performanceOptions);

        }

        /// <summary>
        /// Start creating branches, each branch can contain filtering, mapping and sink of the data
        /// </summary>
        /// <param name="name">Name of the branch</param>
        /// <param name="config">Action of configuring the branch</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        public IStreamBuilder<TIn, TCurrent> AddBranch(string name, Action<IBranchStreamBuilder<TIn, TCurrent>> config)
        {
            return AddBranchCore(name, null, config);
        }

        /// <summary>
        /// Adds a branch that receives only the items matching <paramref name="condition"/>.
        /// The main pipeline still receives every item.
        /// </summary>
        /// <param name="name">Name of the branch</param>
        /// <param name="condition">Predicate deciding which items enter the branch</param>
        /// <param name="config">Action of configuring the branch</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        public IStreamBuilder<TIn, TCurrent> AddBranch(string name, Func<TCurrent, bool> condition, Action<IBranchStreamBuilder<TIn, TCurrent>> config)
        {
            if (condition == null)
            {
                throw new ArgumentNullException(nameof(condition));
            }

            return AddBranchCore(name, condition, config);
        }

        private IStreamBuilder<TIn, TCurrent> AddBranchCore(string name, Func<TCurrent, bool> condition, Action<IBranchStreamBuilder<TIn, TCurrent>> config)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            }
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            // Initialize the fork operator if it's not already
            if (_forkOperator == null)
            {
                _forkOperator = new ForkOperator<TCurrent>();

                if (_firstOperator == null)
                {
                    _firstOperator = _forkOperator;
                    _lastOperator = _forkOperator;
                }
                else
                {
                    _lastOperator.SetNext(_forkOperator);
                    _lastOperator = _forkOperator;
                }
            }

            var branchOperator = BranchStreamBuilder<TIn, TCurrent>.BuildBranch(_name, name, condition, config);
            _forkOperator.AddBranch(name, branchOperator);
            _branchOperators.Add(branchOperator);

            return this;
        }

        /// <summary>
        /// Creates a fan-out pattern to send data to multiple sinks simultaneously.
        /// </summary>
        /// <param name="config">An action to configure the fan-out sinks.</param>
        /// <returns>A fan-out builder to configure and build the stream.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="config"/> is null.</exception>
        /// <remarks>
        /// FanOut provides a simpler API than AddBranch when you need to send the same data
        /// to multiple sinks without complex per-branch transformations.
        /// </remarks>
        /// <example>
        /// <code>
        /// var stream = StreamBuilder&lt;Order&gt;.CreateNewStream("OrderProcessor")
        ///     .Stream()
        ///     .FanOut(fanOut => fanOut
        ///         .To("database", order => SaveToDatabase(order))
        ///         .To("kafka", order => PublishToKafka(order)))
        ///     .Build();
        /// </code>
        /// </example>
        public IFanOutBuilder<TIn, TCurrent> FanOut(Action<IFanOutBuilder<TIn, TCurrent>> config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var fanOutBuilder = new FanOutBuilder<TIn, TCurrent>(
                _name,
                _firstOperator,
                _lastOperator,
                _telemetryProvider,
                _executionOptions,
                _performanceOptions);

            config(fanOutBuilder);

            return fanOutBuilder;
        }

        /// <summary>
        /// Splits the stream into branches and merges their outputs back into one stream.
        /// </summary>
        /// <typeparam name="TNext">The type of the merged items.</typeparam>
        /// <param name="config">An action that adds the merge branches.</param>
        /// <returns>A stream builder for the merged stream.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        public IStreamBuilder<TIn, TNext> Merge<TNext>(Action<IMergeBuilder<TIn, TCurrent, TNext>> config)
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var forkOperator = new ForkOperator<TCurrent>(acceptsRuntimeBranches: false);
            var mergeOperator = new MergeOperator<TNext>();
            var mergeBuilder = new MergeBuilder<TIn, TCurrent, TNext>(_name, forkOperator, mergeOperator);
            config(mergeBuilder);

            if (mergeBuilder.BranchCount == 0)
            {
                throw new InvalidOperationException("Merge must have at least one branch. Use .Branch() to add branches.");
            }

            // Attach only after the configuration succeeded, so a failed Merge leaves the pipeline unchanged
            if (_firstOperator == null)
            {
                _firstOperator = forkOperator;
            }
            else
            {
                _lastOperator.SetNext(forkOperator);
            }
            _lastOperator = mergeOperator;

            return new StreamBuilder<TIn, TNext>(_name, _firstOperator, mergeOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        public IStreamBuilder<TIn, TCurrent> GroupBySilently<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, States.IDataStore<TKey, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"GroupByStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<TKey, List<TCurrent>>(stateStoreName);
            }

            var groupByOperator = new GroupByKeySilentlyOperator<TCurrent, TKey>(keySelector, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = groupByOperator;
                _lastOperator = groupByOperator;
            }
            else
            {
                _lastOperator.SetNext(groupByOperator);
                _lastOperator = groupByOperator;
            }

            return new StreamBuilder<TIn, TCurrent>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        public IStreamBuilder<TIn, TCurrent> AggregateSilently<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, States.IDataStore<TKey, TAggregate> stateStore = null)
        {
            //private readonly Func<TInput, TKey> _keySelector
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"AggregateStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<TKey, TAggregate>(stateStoreName);
            }

            var aggregateOperator = new AggregateSilentlyOperator<TKey, TCurrent, TAggregate>(keySelector, aggregateFunction, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = aggregateOperator;
                _lastOperator = aggregateOperator;
            }
            else
            {
                _lastOperator.SetNext(aggregateOperator);
                _lastOperator = aggregateOperator;
            }

            //return new StreamBuilder<TIn, KeyValuePair<TKey, TAggregate>>(_name, _firstOperator, _lastOperator, _sourceAdded);
            return new StreamBuilder<TIn, TCurrent>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }


        public IStreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>> GroupBy<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, IDataStore<TKey, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"GroupByStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<TKey, List<TCurrent>>(stateStoreName);
            }

            var groupByOperator = new GroupByKeyOperator<TCurrent, TKey>(keySelector, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = groupByOperator;
                _lastOperator = groupByOperator;
            }
            else
            {
                _lastOperator.SetNext(groupByOperator);
                _lastOperator = groupByOperator;
            }

            return new StreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        public IStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>> Aggregate<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, IDataStore<TKey, TAggregate> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"AggregateStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<TKey, TAggregate>(stateStoreName);
            }

            var aggregateOperator = new AggregateOperator<TKey, TCurrent, TAggregate>(keySelector, aggregateFunction, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = aggregateOperator;
                _lastOperator = aggregateOperator;
            }
            else
            {
                _lastOperator.SetNext(aggregateOperator);
                _lastOperator = aggregateOperator;
            }

            return new StreamBuilder<TIn, KeyValuePair<TKey, TAggregate>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        IInitialStreamBuilder<TIn> IInitialStreamBuilder<TIn>.WithTelemetry(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;
            return this;
        }

        public IStreamBuilder<TIn, TCurrent> SetNext(IOperator customOperator)
        {
            if (_firstOperator == null)
            {
                _firstOperator = customOperator;
                _lastOperator = customOperator;
            }
            else
            {
                _lastOperator.SetNext(customOperator);
                _lastOperator = customOperator;
            }

            return this; // Returns the current builder for method chaining
        }

        public IStreamBuilder<TIn, TNext> FlatMap<TNext>(Func<TCurrent, IEnumerable<TNext>> flatMapFunction)
        {
            var flatMapOperator = new FlatMapOperator<TCurrent, TNext>(flatMapFunction);

            if (_firstOperator == null)
            {
                _firstOperator = flatMapOperator;
                _lastOperator = flatMapOperator;
            }
            else
            {
                _lastOperator.SetNext(flatMapOperator);
                _lastOperator = flatMapOperator;
            }

            return new StreamBuilder<TIn, TNext>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, TResult> Join<TRight, TKey, TResult>(
            IDataStore<TKey, TRight> rightStateStore,
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, TRight, TResult> joinFunction)
        {
            var joinOperator = new StreamTableJoinOperator<TCurrent, TRight, TKey, TResult>(
                keySelector,
                joinFunction,
                rightStateStore);

            if (_firstOperator == null)
            {
                _firstOperator = joinOperator;
                _lastOperator = joinOperator;
            }
            else
            {
                _lastOperator.SetNext(joinOperator);
                _lastOperator = joinOperator;
            }

            return new StreamBuilder<TIn, TResult>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, TResult> LeftJoin<TRight, TKey, TResult>(
            IDataStore<TKey, TRight> rightStateStore,
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, TRight, TResult> joinFunction)
        {
            var joinOperator = new StreamTableLeftJoinOperator<TCurrent, TRight, TKey, TResult>(
                keySelector,
                joinFunction,
                rightStateStore);

            if (_firstOperator == null)
            {
                _firstOperator = joinOperator;
                _lastOperator = joinOperator;
            }
            else
            {
                _lastOperator.SetNext(joinOperator);
                _lastOperator = joinOperator;
            }

            return new StreamBuilder<TIn, TResult>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        /// <summary>
        /// Performs a windowed join between the current stream (left) and another stream (right) based on a shared key.
        /// Elements from both streams are buffered within the configured time window and matched when they share the same key.
        /// </summary>
        /// <typeparam name="TRight">The type of elements in the right stream.</typeparam>
        /// <typeparam name="TKey">The type of the key used for matching elements from both streams.</typeparam>
        /// <typeparam name="TResult">The type of the result produced by joining matched elements.</typeparam>
        /// <param name="joinOperator">
        /// The stream-stream join operator that handles windowed buffering and matching.
        /// Call its <c>ProcessRight</c> method to feed elements from the right stream.
        /// </param>
        /// <returns>
        /// An <see cref="IStreamBuilder{TIn, TResult}"/> representing the pipeline after the stream-stream join.
        /// </returns>
        public IStreamBuilder<TIn, TResult> JoinStream<TRight, TKey, TResult>(
            StreamStreamJoinOperator<TCurrent, TRight, TKey, TResult> joinOperator)
        {
            if (joinOperator == null)
                throw new ArgumentNullException(nameof(joinOperator));

            if (_firstOperator == null)
            {
                _firstOperator = joinOperator;
                _lastOperator = joinOperator;
            }
            else
            {
                _lastOperator.SetNext(joinOperator);
                _lastOperator = joinOperator;
            }

            return new StreamBuilder<TIn, TResult>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> TumblingWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan windowSize,
            string stateStoreName = null,
            IDataStore<string, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"TumblingWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, List<TCurrent>>(stateStoreName);
            }

            var windowOperator = new TumblingWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, windowSize, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> SlidingWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan windowSize,
            TimeSpan slideInterval,
            string stateStoreName = null,
            IDataStore<string, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"SlidingWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, List<TCurrent>>(stateStoreName);
            }

            var windowOperator = new SlidingWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, windowSize, slideInterval, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> SessionWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan inactivityGap,
            string stateStoreName = null,
            IDataStore<string, SessionState<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"SessionWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, SessionState<TCurrent>>(stateStoreName);
            }

            var windowOperator = new SessionWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, inactivityGap, stateStore);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedTumblingWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan windowSize,
            WindowConfiguration<TCurrent> config,
            string stateStoreName = null,
            IDataStore<string, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"AdvancedTumblingWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, List<TCurrent>>(stateStoreName);
            }

            var windowOperator = new AdvancedTumblingWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, windowSize, stateStore, config);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedSlidingWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan windowSize,
            TimeSpan slideInterval,
            WindowConfiguration<TCurrent> config,
            string stateStoreName = null,
            IDataStore<string, List<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"AdvancedSlidingWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, List<TCurrent>>(stateStoreName);
            }

            var windowOperator = new AdvancedSlidingWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, windowSize, slideInterval, stateStore, config);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

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
        public IStreamBuilder<TIn, WindowResult<string, TCurrent>> AdvancedSessionWindow<TKey>(
            Func<TCurrent, TKey> keySelector,
            Func<TCurrent, DateTime> timestampSelector,
            TimeSpan inactivityGap,
            WindowConfiguration<TCurrent> config,
            string stateStoreName = null,
            IDataStore<string, AdvancedSessionState<TCurrent>> stateStore = null)
        {
            if (stateStore == null)
            {
                if (string.IsNullOrEmpty(stateStoreName))
                {
                    stateStoreName = $"AdvancedSessionWindowStateStore_{Guid.NewGuid()}";
                }
                stateStore = new InMemoryStateStore<string, AdvancedSessionState<TCurrent>>(stateStoreName);
            }

            var windowOperator = new AdvancedSessionWindowOperator<TCurrent, TKey>(keySelector, timestampSelector, inactivityGap, stateStore, config);

            if (_firstOperator == null)
            {
                _firstOperator = windowOperator;
                _lastOperator = windowOperator;
            }
            else
            {
                _lastOperator.SetNext(windowOperator);
                _lastOperator = windowOperator;
            }

            return new StreamBuilder<TIn, WindowResult<string, TCurrent>>(_name, _firstOperator, _lastOperator, _sourceAdded, _telemetryProvider, _executionOptions, _performanceOptions);
        }

        IInitialStreamBuilder<TIn> IInitialStreamBuilder<TIn>.WithErrorHandling(StreamExecutionOptions executionOptions)
        {
            _executionOptions = executionOptions ?? StreamExecutionOptions.Default;
            _executionOptions.StreamName = _name;
            return this;
        }

        IInitialStreamBuilder<TIn> IInitialStreamBuilder<TIn>.WithPerformanceOptions(StreamPerformanceOptions performanceOptions)
        {
            _performanceOptions = performanceOptions ?? StreamPerformanceOptions.Default;
            return this;
        }
    }
}
