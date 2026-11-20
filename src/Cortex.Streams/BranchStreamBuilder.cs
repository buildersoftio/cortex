using Cortex.States;
using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Streams.Operators.Windows;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    /// <summary>
    /// Builds a branch within the stream processing pipeline.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the branch.</typeparam>
    public class BranchStreamBuilder<TIn, TCurrent> : IBranchStreamBuilder<TIn, TCurrent>
    {
        private readonly string _name;
        internal IOperator _firstOperator;
        internal IOperator _lastOperator;
        private bool _sourceAdded = false;

        public BranchStreamBuilder(string name)
        {
            _name = name;
        }

        /// <summary>
        /// Builds a named branch from <paramref name="config"/>. When <paramref name="condition"/> is set,
        /// a filter heads the branch so only matching items enter it.
        /// </summary>
        /// <exception cref="InvalidOperationException">The configuration added no operators.</exception>
        internal static BranchOperator<TCurrent> BuildBranch(
            string streamName,
            string branchName,
            Func<TCurrent, bool> condition,
            Action<IBranchStreamBuilder<TIn, TCurrent>> config)
        {
            var branchBuilder = new BranchStreamBuilder<TIn, TCurrent>(streamName);
            config(branchBuilder);

            if (branchBuilder._firstOperator == null)
            {
                throw new InvalidOperationException($"Branch '{branchName}' must have at least one operator.");
            }

            var firstOperator = branchBuilder._firstOperator;
            if (condition != null)
            {
                var conditionOperator = new FilterOperator<TCurrent>(condition);
                conditionOperator.SetNext(firstOperator);
                firstOperator = conditionOperator;
            }

            return new BranchOperator<TCurrent>(branchName, firstOperator);
        }

        /// <summary>
        /// Adds a filter operator to the branch.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The branch stream builder for method chaining.</returns>
        public IBranchStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate)
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

            return this;
        }


        /// <summary>
        /// Adds a map operator to the branch to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The branch stream builder with the new data type.</returns>
        public IBranchStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction)
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

            return new BranchStreamBuilder<TIn, TNext>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator
            };
        }

        /// <summary>
        /// Adds a sink function to the branch to consume data.
        /// </summary>
        /// <param name="sinkFunction">An action to consume data.</param>
        public void Sink(Action<TCurrent> sinkFunction)
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
        }

        /// <summary>
        /// Adds a sink operator to the branch to consume data.
        /// </summary>
        /// <param name="sinkOperator">A sink operator to consume data.</param>
        public void Sink(ISinkOperator<TCurrent> sinkOperator)
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
        }


        public IBranchStreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>> GroupBy<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, States.IDataStore<TKey, List<TCurrent>> stateStore = null)
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

            return new BranchStreamBuilder<TIn, KeyValuePair<TKey, List<TCurrent>>>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded
            };
        }

        public IBranchStreamBuilder<TIn, TCurrent> GroupBySilently<TKey>(Func<TCurrent, TKey> keySelector, string stateStoreName = null, States.IDataStore<TKey, List<TCurrent>> stateStore = null)
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

            return new BranchStreamBuilder<TIn, TCurrent>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded
            };
        }

        public IBranchStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>> Aggregate<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, States.IDataStore<TKey, TAggregate> stateStore = null)
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

            return new BranchStreamBuilder<TIn, KeyValuePair<TKey, TAggregate>>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded
            };
        }

        public IBranchStreamBuilder<TIn, TCurrent> AggregateSilently<TKey, TAggregate>(Func<TCurrent, TKey> keySelector, Func<TAggregate, TCurrent, TAggregate> aggregateFunction, string stateStoreName = null, States.IDataStore<TKey, TAggregate> stateStore = null)
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

            return new BranchStreamBuilder<TIn, TCurrent>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded
            };
        }

        public IBranchStreamBuilder<TIn, TNext> FlatMap<TNext>(Func<TCurrent, IEnumerable<TNext>> flatMapFunction)
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

            return new BranchStreamBuilder<TIn, TNext>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded
            };
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
        public IBranchStreamBuilder<TIn, TResult> Join<TRight, TKey, TResult>(
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

            return new BranchStreamBuilder<TIn, TResult>(_name)
                        {
                            _firstOperator = _firstOperator,
                            _lastOperator = _lastOperator,
                            _sourceAdded = _sourceAdded,
                        };
                    }

        /// <summary>
        /// Performs a left join between the current branch stream and a state-backed table (right side) based on a shared key.
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
        /// An <see cref="IBranchStreamBuilder{TIn, TResult}"/> representing the pipeline after the left join operation.
        /// </returns>
        public IBranchStreamBuilder<TIn, TResult> LeftJoin<TRight, TKey, TResult>(
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

            return new BranchStreamBuilder<TIn, TResult>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded,
            };
        }

        /// <summary>
        /// Performs a windowed join between the current branch stream (left) and another stream (right) based on a shared key.
        /// </summary>
        /// <typeparam name="TRight">The type of elements in the right stream.</typeparam>
        /// <typeparam name="TKey">The type of the key used for matching elements from both streams.</typeparam>
        /// <typeparam name="TResult">The type of the result produced by joining matched elements.</typeparam>
        /// <param name="joinOperator">The stream-stream join operator that handles windowed buffering and matching.</param>
        /// <returns>An <see cref="IBranchStreamBuilder{TIn, TResult}"/> representing the pipeline after the stream-stream join.</returns>
        public IBranchStreamBuilder<TIn, TResult> JoinStream<TRight, TKey, TResult>(
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

            return new BranchStreamBuilder<TIn, TResult>(_name)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator,
                _sourceAdded = _sourceAdded,
            };
        }

                    /// <summary>
                    /// Applies a tumbling window to the branch. Tumbling windows are fixed-size, non-overlapping windows.
                    /// </summary>
                    /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                    /// <param name="keySelector">A function to extract the key from each input item.</param>
                    /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                    /// <param name="windowSize">The size of each tumbling window.</param>
                    /// <param name="stateStoreName">Optional name for the state store.</param>
                    /// <param name="stateStore">Optional state store to use for storing window data.</param>
                    /// <returns>A branch stream builder emitting window results.</returns>
                    public IBranchStreamBuilder<TIn, WindowResult<string, TCurrent>> TumblingWindow<TKey>(
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

                        return new BranchStreamBuilder<TIn, WindowResult<string, TCurrent>>(_name)
                        {
                            _firstOperator = _firstOperator,
                            _lastOperator = _lastOperator,
                            _sourceAdded = _sourceAdded
                        };
                    }

                    /// <summary>
                    /// Applies a sliding window to the branch. Sliding windows have a fixed size but overlap based on the slide interval.
                    /// </summary>
                    /// <typeparam name="TKey">The type of the key used to partition windows.</typeparam>
                    /// <param name="keySelector">A function to extract the key from each input item.</param>
                    /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                    /// <param name="windowSize">The size of each sliding window.</param>
                    /// <param name="slideInterval">The interval at which the window slides.</param>
                    /// <param name="stateStoreName">Optional name for the state store.</param>
                    /// <param name="stateStore">Optional state store to use for storing window data.</param>
                    /// <returns>A branch stream builder emitting window results.</returns>
                    public IBranchStreamBuilder<TIn, WindowResult<string, TCurrent>> SlidingWindow<TKey>(
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

                        return new BranchStreamBuilder<TIn, WindowResult<string, TCurrent>>(_name)
                        {
                            _firstOperator = _firstOperator,
                            _lastOperator = _lastOperator,
                            _sourceAdded = _sourceAdded
                        };
                    }

                    /// <summary>
                    /// Applies a session window to the branch. Session windows group events by activity sessions separated by inactivity gaps.
                    /// </summary>
                    /// <typeparam name="TKey">The type of the key used to partition sessions.</typeparam>
                    /// <param name="keySelector">A function to extract the key from each input item.</param>
                    /// <param name="timestampSelector">A function to extract the timestamp from each input item.</param>
                    /// <param name="inactivityGap">The duration of inactivity after which a session is closed.</param>
                    /// <param name="stateStoreName">Optional name for the state store.</param>
                    /// <param name="stateStore">Optional state store to use for storing session data.</param>
                    /// <returns>A branch stream builder emitting window results.</returns>
                    public IBranchStreamBuilder<TIn, WindowResult<string, TCurrent>> SessionWindow<TKey>(
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

                        return new BranchStreamBuilder<TIn, WindowResult<string, TCurrent>>(_name)
                        {
                            _firstOperator = _firstOperator,
                            _lastOperator = _lastOperator,
                            _sourceAdded = _sourceAdded
                        };
                    }
                }
            }
