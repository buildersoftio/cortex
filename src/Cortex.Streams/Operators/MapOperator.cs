using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that transforms data from one type to another.
    /// </summary>
    /// <typeparam name="TInput">The input data type.</typeparam>
    /// <typeparam name="TOutput">The output data type after transformation.</typeparam>
    public class MapOperator<TInput, TOutput> : IOperator, IHasNextOperators
    {
        private readonly Func<TInput, TOutput> _mapFunction;
        private IOperator _nextOperator;

        public MapOperator(Func<TInput, TOutput> mapFunction)
        {
            _mapFunction = mapFunction;
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
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
