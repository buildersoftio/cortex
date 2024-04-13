using Cortex.Core.Sources;

namespace Cortex.Core.Streams
{
    public abstract class StreamBase : IStreamBuilder, IStream
    {

        private ISourceDesigner? _source;

        protected StreamBase()
        {
            _source = null;
        }

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


        public ISourceDesigner<TIn> From<TIn>()
        {
            _source = new SourceBase<TIn>();
            return _source as ISourceDesigner<TIn>;
        }
    }
}
