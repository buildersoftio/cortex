using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class BranchOperator<T> : IOperator, IHasNextOperators
    {
        private readonly string _branchName;
        private readonly IOperator _branchOperator;

        public BranchOperator(string branchName, IOperator branchOperator)
        {
            _branchName = branchName;
            _branchOperator = branchOperator;
        }

        public string BranchName => _branchName;

        public void Process(object input)
        {
            _branchOperator.Process(input);
        }

        public void SetNext(IOperator nextOperator)
        {
            throw new InvalidOperationException("Cannot set next operator on a BranchOperator.");
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_branchOperator != null)
                yield return _branchOperator;
        }
    }
}
