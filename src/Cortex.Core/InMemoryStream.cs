using Cortex.Core.Streams;

namespace Cortex.Core.Streams
{
    public class InMemoryStream : StreamBase, IStream
    {
        public void Emit(object value)
        {
            throw new NotImplementedException();
        }

        public void Emit<T>(T value)
        {
            throw new NotImplementedException();
        }

        public void Run()
        {
            throw new NotImplementedException();
        }

        public void RunAsync()
        {
            throw new NotImplementedException();
        }

        public void RunOnes()
        {
            throw new NotImplementedException();
        }

        public void RunOnesAsync()
        {
            throw new NotImplementedException();
        }
    }
}
