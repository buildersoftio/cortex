using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;

namespace Cortex.Streams
{
    // Sink builder that only allows Build
    public class SinkBuilder<TIn> : ISinkBuilder<TIn>
    {
        private readonly string _name;
        private readonly IOperator _firstOperator;

        public SinkBuilder(string name, IOperator firstOperator)
        {
            _name = name;
            _firstOperator = firstOperator;
        }

        public Stream<TIn> Build()
        {
            return new Stream<TIn>(_name, _firstOperator);
        }
    }
}
