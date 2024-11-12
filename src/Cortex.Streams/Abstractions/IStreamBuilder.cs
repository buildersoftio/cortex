using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Abstractions
{
    public interface IStreamBuilder<TIn, TCurrent>
    {
        IStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);
        IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);
        ISinkBuilder<TIn, TCurrent> Sink(Action<TCurrent> sinkFunction);
        ISinkBuilder<TIn, TCurrent> Sink(ISinkOperator<TCurrent> sinkOperator);
        IStreamBuilder<TIn, TCurrent> AddBranch(string name, Action<IBranchStreamBuilder<TIn, TCurrent>> config);
        IStream<TIn, TCurrent> Build();

    }
}
