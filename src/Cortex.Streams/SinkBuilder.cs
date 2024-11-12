using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using System.Collections.Generic;

namespace Cortex.Streams
{
    // Sink builder that only allows Build
    public class SinkBuilder<TIn, TCurrent> : ISinkBuilder<TIn, TCurrent>
    {
        private readonly string _name;
        private readonly IOperator _firstOperator;
        private readonly List<BranchOperator<TCurrent>> _branchOperators;

        public SinkBuilder(string name, IOperator firstOperator, List<BranchOperator<TCurrent>> branchOperators)
        {
            _name = name;
            _firstOperator = firstOperator;
            _branchOperators = branchOperators;
        }

        public IStream<TIn, TCurrent> Build()
        {
            return new Stream<TIn, TCurrent>(_name, _firstOperator, _branchOperators);
        }
    }
}
