using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    public class StreamBuilder<TIn, TCurrent> : IInitialStreamBuilder<TIn, TCurrent>, IStreamBuilder<TIn, TCurrent>
    {
        private readonly string _name;
        private IOperator _firstOperator;
        private IOperator _lastOperator;
        private bool _sourceAdded = false;
        private readonly List<BranchOperator<TCurrent>> _branchOperators = new List<BranchOperator<TCurrent>>();
        private ForkOperator<TCurrent> _forkOperator;



        private StreamBuilder(string name)
        {
            _name = name;
        }

        private StreamBuilder(string name, IOperator firstOperator, IOperator lastOperator, bool sourceAdded)
        {
            _name = name;
            _firstOperator = firstOperator;
            _lastOperator = lastOperator;
            _sourceAdded = sourceAdded;
        }

        public static IInitialStreamBuilder<TIn, TIn> CreateNewStream(string name)
        {
            return new StreamBuilder<TIn, TIn>(name);
        }

        public static IStreamBuilder<TIn, TCurrent> CreateNewStream(string name, IOperator firstOperator, IOperator lastOperator)
        {
            return new StreamBuilder<TIn, TCurrent>(name, firstOperator, lastOperator, false);
        }

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

            return new StreamBuilder<TIn, TNext>(_name, _firstOperator, _lastOperator, _sourceAdded);
        }

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

            return new SinkBuilder<TIn, TCurrent>(_name, _firstOperator, _branchOperators);
        }

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

            return new SinkBuilder<TIn, TCurrent>(_name, _firstOperator, _branchOperators);
        }

        public IStreamBuilder<TIn, TCurrent> Stream(ISourceOperator<TCurrent> sourceOperator)
        {
            if (_sourceAdded)
            {
                throw new InvalidOperationException("Source operator already added.");
            }

            var sourceAdapter = new SourceOperatorAdapter<TCurrent>(sourceOperator);

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
            return this; // Returns IStreamBuilder<TIn, TCurrent>
        }

        public IStreamBuilder<TIn, TCurrent> Stream()
        {
            // In memory source added.
            if (_sourceAdded)
            {
                throw new InvalidOperationException("Source operator already added.");
            }

            _sourceAdded = true;
            return this; // Returns IStreamBuilder<TIn, TCurrent>
        }

        public IStreamBuilder<TIn, TCurrent> Branch(params (string name, Action<IStreamBuilder<TIn, TCurrent>>)[] branches)
        {
            // Old implementation; please remove it

            if (branches == null || branches.Length == 0)
            {
                throw new ArgumentException("At least one branch must be provided.");
            }

            var forkOperator = new ForkOperator<TCurrent>();

            if (_firstOperator == null)
            {
                _firstOperator = forkOperator;
                _lastOperator = forkOperator;
            }
            else
            {
                _lastOperator.SetNext(forkOperator);
                _lastOperator = forkOperator;
            }

            foreach (var (name, branchAction) in branches)
            {
                if (string.IsNullOrEmpty(name))
                {
                    throw new ArgumentException("Branch name cannot be null or empty.");
                }

                var branchBuilder = new StreamBuilder<TIn, TCurrent>(_name);
                branchAction(branchBuilder);

                if (branchBuilder._firstOperator == null)
                {
                    throw new InvalidOperationException($"Branch '{name}' must have at least one operator.");
                }

                var branchOperator = new BranchOperator<TCurrent>(name, branchBuilder._firstOperator);
                forkOperator.AddBranch(name, branchOperator);
                _branchOperators.Add(branchOperator);
            }

            return this;
        }

        public IStream<TIn, TCurrent> Build()
        {
            return new Stream<TIn, TCurrent>(_name, _firstOperator, _branchOperators);
        }

        public IStreamBuilder<TIn, TCurrent> AddBranch(string name, Action<IBranchStreamBuilder<TIn, TCurrent>> config)
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

            // Create a new branch builder
            var branchBuilder = new BranchStreamBuilder<TIn, TCurrent>(_name);
            config(branchBuilder);

            if (branchBuilder._firstOperator == null)
            {
                throw new InvalidOperationException($"Branch '{name}' must have at least one operator.");
            }

            var branchOperator = new BranchOperator<TCurrent>(name, branchBuilder._firstOperator);
            _forkOperator.AddBranch(name, branchOperator);
            _branchOperators.Add(branchOperator);

            return this;
        }
    }
}
