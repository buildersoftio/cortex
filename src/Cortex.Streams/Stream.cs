using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    /// <summary>
    /// Represents a built stream that can be started and stopped.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
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

        /// <summary>
        /// Starts the stream processing.
        /// </summary>
        public void Start()
        {
            _isStarted = true;
        }

        /// <summary>
        /// Stops the stream processing.
        /// </summary>
        public void Stop()
        {
            _isStarted = false;

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }
        }

        /// <summary>
        /// Gets the current status of the stream.
        /// </summary>
        /// <returns>A string indicating whether the stream is running or stopped.</returns>
        public string GetStatus()
        {
            return _isStarted ? "Running" : "Stopped";
        }

        /// <summary>
        /// Emits data into the stream when no source operator is used.
        /// </summary>
        /// <param name="value">The data to emit.</param>
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
