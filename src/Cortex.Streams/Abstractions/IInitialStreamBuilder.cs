using System;

namespace Cortex.Streams.Abstractions
{
    public interface IInitialStreamBuilder<TIn, TCurrent>
    {
        IStreamBuilder<TIn, TCurrent> Stream();
        IStreamBuilder<TIn, TCurrent> Stream(ISourceOperator<TCurrent> sourceOperator);
        IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);
    }
}
