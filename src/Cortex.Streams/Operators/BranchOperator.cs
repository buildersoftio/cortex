using System;

namespace Cortex.Streams.Operators
{
    public class BranchOperator<T> : IOperator
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
    }
}
