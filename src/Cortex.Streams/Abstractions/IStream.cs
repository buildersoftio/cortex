namespace Cortex.Streams
{
    public interface IStream<TIn>
    {
        void Start();
        void Stop();
        void Emit(TIn value);
        string GetStatus();
    }
}
