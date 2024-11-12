namespace Cortex.Streams.Operators
{
    public interface ISinkOperator<TInput>
    {
        void Start();
        void Process(TInput input);
        void Stop();
    }
}
