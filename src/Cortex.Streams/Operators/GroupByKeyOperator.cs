using Cortex.States;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class GroupByKeyOperator<TInput, TKey> : IOperator
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly IStateStore<TKey, List<TInput>> _stateStore;
        private IOperator _nextOperator;

        public GroupByKeyOperator(Func<TInput, TKey> keySelector, IStateStore<TKey, List<TInput>> stateStore)
        {
            _keySelector = keySelector;
            _stateStore = stateStore;
        }

        public void Process(object input)
        {
            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);

            var group = _stateStore.Get(key) ?? new List<TInput>();
            group.Add(typedInput);
            _stateStore.Put(key, group);

            _nextOperator?.Process(new KeyValuePair<TKey, List<TInput>>(key, group));
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
