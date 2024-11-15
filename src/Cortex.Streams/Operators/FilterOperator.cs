﻿using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that filters data based on a predicate.
    /// </summary>
    /// <typeparam name="T">The type of data being filtered.</typeparam>
    public class FilterOperator<T> : IOperator, IHasNextOperators
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

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
        }
    }
}
