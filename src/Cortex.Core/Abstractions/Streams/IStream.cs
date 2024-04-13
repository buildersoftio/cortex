namespace Cortex.Core.Streams
{
    public interface IStream
    {
        void Run();
        void RunOnes();
        void RunAsync();
        void RunOnesAsync();

        void Emit(object value);
        void Emit<T>(T value);
    }
}
