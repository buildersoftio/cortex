using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Abstractions
{
    public interface ISourceStreamBuilder<TOut>
    {
        IStreamBuilder<TOut, TOut> Map<TNext>(Func<TOut, TNext> mapFunction);
        ISinkBuilder<TOut> Sink(Action<TOut> sinkFunction);
        ISinkBuilder<TOut> ToSink(ISinkOperator<TOut> sinkOperator);
        Stream<TOut> Build();
    }
}
