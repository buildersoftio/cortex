using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class SourceOperatorAdapter<TOutput> : IOperator, IHasNextOperators
    {
        private readonly ISourceOperator<TOutput> _sourceOperator;
        private IOperator _nextOperator;

        public SourceOperatorAdapter(ISourceOperator<TOutput> sourceOperator)
        {
            _sourceOperator = sourceOperator;
        }

        public void Process(object input)
        {
            // Source operator starts the data flow, so Process is not used.
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
            _sourceOperator.Start(output =>
            {
                _nextOperator?.Process(output);
            });
        }

        public void Stop()
        {
            _sourceOperator.Stop();
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
        }
    }
}
