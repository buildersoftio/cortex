using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Abstractions
{
    public interface IBranchStreamBuilder<TIn, TCurrent>
    {
        IBranchStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);
        IBranchStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);
        void Sink(Action<TCurrent> sinkFunction);
        void Sink(ISinkOperator<TCurrent> sinkOperator);

    }
}
