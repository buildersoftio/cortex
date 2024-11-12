using System;

namespace Cortex.Streams.Operators
{
    public class FilterOperator<T> : IOperator
    {
        private readonly Func<T, bool> _predicate;
        private IOperator _nextOperator;

        public FilterOperator(Func<T, bool> predicate)
        {
            _predicate = predicate;
        }

        public void Process(object input)
        {
            T typedInput = (T)input;
            if (_predicate(typedInput))
            {
                _nextOperator?.Process(typedInput);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
