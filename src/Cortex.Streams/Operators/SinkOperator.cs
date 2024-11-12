using System;

namespace Cortex.Streams.Operators
{
    public class SinkOperator<TInput> : IOperator
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
    }

}
