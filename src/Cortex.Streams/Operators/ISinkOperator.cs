namespace Cortex.Streams.Operators
{
    public interface ISinkOperator<TInput>
    {
        void Process(TInput input);
        void Start();
        void Stop();
    }
}
