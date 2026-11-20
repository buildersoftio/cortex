using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams
{
    /// <summary>
    /// Builds the branches of a merge: each branch starts at a shared fork and ends at a shared <see cref="MergeOperator{T}"/>.
    /// </summary>
    internal class MergeBuilder<TIn, TCurrent, TNext> : IMergeBuilder<TIn, TCurrent, TNext>
    {
        private readonly string _streamName;
        private readonly ForkOperator<TCurrent> _forkOperator;
        private readonly MergeOperator<TNext> _mergeOperator;

        internal MergeBuilder(string streamName, ForkOperator<TCurrent> forkOperator, MergeOperator<TNext> mergeOperator)
        {
            _streamName = streamName;
            _forkOperator = forkOperator;
            _mergeOperator = mergeOperator;
        }

        internal int BranchCount { get; private set; }

        public IMergeBuilder<TIn, TCurrent, TNext> Branch(
            string name,
            Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config)
        {
            return AddBranch(name, null, config);
        }

        public IMergeBuilder<TIn, TCurrent, TNext> Branch(
            string name,
            Func<TCurrent, bool> condition,
            Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config)
        {
            if (condition == null)
                throw new ArgumentNullException(nameof(condition));

            return AddBranch(name, condition, config);
        }

        private IMergeBuilder<TIn, TCurrent, TNext> AddBranch(
            string name,
            Func<TCurrent, bool> condition,
            Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (_forkOperator.Branches.ContainsKey(name))
                throw new ArgumentException($"A branch named '{name}' has already been added to this merge.", nameof(name));

            var branchBuilder = new BranchStreamBuilder<TIn, TCurrent>(_streamName);
            var branchEnd = config(branchBuilder) as BranchStreamBuilder<TIn, TNext>;

            IOperator firstOperator;
            if (branchEnd != null && ReferenceEquals(branchEnd, branchBuilder) && branchBuilder._firstOperator == null)
            {
                // Pass-through branch: items go straight to the merge
                firstOperator = _mergeOperator;
            }
            else if (branchEnd != null
                && branchBuilder._firstOperator != null
                && ReferenceEquals(branchEnd._firstOperator, branchBuilder._firstOperator))
            {
                branchEnd._lastOperator.SetNext(_mergeOperator);
                firstOperator = branchBuilder._firstOperator;
            }
            else
            {
                throw new ArgumentException(
                    $"The configuration of merge branch '{name}' must return the branch builder it was given, or one returned by its operators.",
                    nameof(config));
            }

            if (condition != null)
            {
                var conditionOperator = new FilterOperator<TCurrent>(condition);
                conditionOperator.SetNext(firstOperator);
                firstOperator = conditionOperator;
            }

            _forkOperator.AddBranch(name, new BranchOperator<TCurrent>(name, firstOperator));
            BranchCount++;

            return this;
        }
    }
}
