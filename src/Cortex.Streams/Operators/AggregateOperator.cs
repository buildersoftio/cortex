using Cortex.States;
using Cortex.States.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class AggregateOperator<TKey, TInput, TAggregate> : IOperator, IStatefulOperator
    {
        private readonly Func<TInput, TKey> _keySelector;

        private readonly Func<TAggregate, TInput, TAggregate> _aggregateFunction;
        private readonly IStateStore<TKey, TAggregate> _stateStore;
        private IOperator _nextOperator;

        public AggregateOperator(Func<TInput, TKey> keySelector, Func<TAggregate, TInput, TAggregate> aggregateFunction, IStateStore<TKey, TAggregate> stateStore)
        {
            _keySelector = keySelector;

            _aggregateFunction = aggregateFunction;
            _stateStore = stateStore;
        }

        public void Process(object input)
        {
            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);

            TAggregate aggregate;
            lock (_stateStore)
            {
                aggregate = _stateStore.Get(key);
                aggregate = _aggregateFunction(aggregate, typedInput);
                _stateStore.Put(key, aggregate);
            }

            _nextOperator?.Process(new KeyValuePair<TKey, TAggregate>(key, aggregate));
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }

        public IEnumerable<IStateStore> GetStateStores()
        {
            yield return _stateStore;
        }

    }
}
