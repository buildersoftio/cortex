using System;

namespace Cortex.Streams.Operators
{
    public class MapOperator<TInput, TOutput> : IOperator
    {
        private readonly Func<TInput, TOutput> _mapFunction;
        private IOperator _nextOperator;

        public MapOperator(Func<TInput, TOutput> mapFunction)
        {
            _mapFunction = mapFunction;
        }

        public void Process(object input)
        {
            var output = _mapFunction((TInput)input);
            _nextOperator?.Process(output);
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
