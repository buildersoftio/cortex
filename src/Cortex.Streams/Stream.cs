using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    public class Stream<TIn, TCurrent> : IStream<TIn, TCurrent>
    {
        private readonly string _name;
        private readonly IOperator _operatorChain;
        private readonly List<BranchOperator<TCurrent>> _branchOperators;
        private bool _isStarted;

        internal Stream(string name, IOperator operatorChain, List<BranchOperator<TCurrent>> branchOperators)
        {
            _name = name;
            _operatorChain = operatorChain;
            _branchOperators = branchOperators;
        }

        public void Start()
        {
            _isStarted = true;
        }

        public void Stop()
        {
            _isStarted = false;

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }
        }

        public string GetStatus()
        {
            return _isStarted ? "Running" : "Stopped";
        }

        public void Emit(TIn value)
        {
            if (_isStarted)
            {
                if (_operatorChain is SourceOperatorAdapter<TIn>)
                {
                    throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");
                }

                _operatorChain.Process(value);
            }
            else
            {
                throw new InvalidOperationException("Stream has not been started.");
            }
        }

        public IReadOnlyDictionary<string, BranchOperator<TCurrent>> GetBranches()
        {
            var branchDict = new Dictionary<string, BranchOperator<TCurrent>>();
            foreach (var branchOperator in _branchOperators)
            {
                branchDict[branchOperator.BranchName] = branchOperator;
            }
            return branchDict;
        }
    }
}
