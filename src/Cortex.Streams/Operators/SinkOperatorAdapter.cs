using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class SinkOperatorAdapter<TInput> : IOperator, IHasNextOperators
    {
        private readonly ISinkOperator<TInput> _sinkOperator;

        public SinkOperatorAdapter(ISinkOperator<TInput> sinkOperator)
        {
            _sinkOperator = sinkOperator;
        }

        public void Process(object input)
        {
            _sinkOperator.Process((TInput)input);
        }

        public void SetNext(IOperator nextOperator)
        {
            // Sink operator is the end; does nothing
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            // Sink operator adapter has no next operator
            yield break;
        }
    }
}
