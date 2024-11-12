using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that consumes data at the end of the stream.
    /// </summary>
    /// <typeparam name="TInput">The type of data consumed by the sink.</typeparam>
    public class SinkOperator<TInput> : IOperator, IHasNextOperators
    {
        private readonly Action<TInput> _sinkFunction;

        public SinkOperator(Action<TInput> sinkFunction)
        {
            _sinkFunction = sinkFunction;
        }

        public void Process(object input)
        {
            _sinkFunction((TInput)input);
        }

        public void SetNext(IOperator nextOperator)
        {
            // Sink operator is the end of the chain; does nothing
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            // Sink operator has no next operator
            yield break;
        }
    }

}
