using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    internal class ForkOperator<T> : IOperator
    {
        private readonly Dictionary<string, BranchOperator<T>> _branches = new Dictionary<string, BranchOperator<T>>();

        public void AddBranch(string name, BranchOperator<T> branchOperator)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            if (branchOperator == null)
                throw new ArgumentNullException(nameof(branchOperator));

            _branches[name] = branchOperator;
        }

        public void Process(object input)
        {
            foreach (var branch in _branches.Values)
            {
                branch.Process(input);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            throw new InvalidOperationException("Cannot set next operator on a ForkOperator.");
        }

        public IReadOnlyDictionary<string, BranchOperator<T>> Branches => _branches;
    }
}
