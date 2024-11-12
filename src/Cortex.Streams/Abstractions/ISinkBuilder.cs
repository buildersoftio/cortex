namespace Cortex.Streams.Abstractions
{
    public interface ISinkBuilder<TIn>
    {
        Stream<TIn> Build();
    }
}
