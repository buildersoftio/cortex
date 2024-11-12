namespace Cortex.Streams.Abstractions
{
    public interface ISinkBuilder<TIn, TCurrent>
    {
        IStream<TIn, TCurrent> Build();
    }
}
