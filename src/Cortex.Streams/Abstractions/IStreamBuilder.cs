using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Abstractions
{
    public interface IStreamBuilder<TIn, TCurrent>
    {
        IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);
        IStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);
        ISinkBuilder<TIn> Sink(Action<TCurrent> sinkFunction);
        ISinkBuilder<TIn> Sink(ISinkOperator<TCurrent> sinkOperator);
    }
}
