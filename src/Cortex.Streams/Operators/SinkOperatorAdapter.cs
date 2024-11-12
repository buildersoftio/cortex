namespace Cortex.Streams.Operators
{
    public class SinkOperatorAdapter<TInput> : IOperator
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
    }
}
