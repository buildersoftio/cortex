using System;
namespace Cortex.Streams.Operators
{
    public interface ISourceOperator<TOutput>
    {
        void Start(Action<TOutput> emit);
        void Stop();
    }
}
