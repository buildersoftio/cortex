using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams
{
    public class Stream<TIn> : IStream<TIn>
    {
        private readonly string _name;
        private readonly IOperator _operatorChain;
        private bool _isStarted;

        public Stream(string name, IOperator operatorChain)
        {
            _name = name;
            _operatorChain = operatorChain;
        }

        public void Start()
        {
            _isStarted = true;
        }

        public void Stop()
        {
            _isStarted = false;

            if (_operatorChain is SourceOperatorAdapter<TIn> sourceAdapter)
            {
                sourceAdapter.Stop();
            }
        }

        public string GetStatus()
        {
            return _isStarted ? "Running" : "Stopped";
        }

        public void Emit(TIn value)
        {
            if (_isStarted)
            {
                if (_operatorChain is SourceOperatorAdapter<TIn>)
                {
                    throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");
                }

                _operatorChain.Process(value);
            }
            else
            {
                throw new InvalidOperationException("Stream has not been started.");
            }
        }
    }
}
