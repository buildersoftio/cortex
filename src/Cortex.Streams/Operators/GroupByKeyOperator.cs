using Cortex.States;
using Cortex.States.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    public class GroupByKeyOperator<TInput, TKey> : IOperator, IStatefulOperator
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly IStateStore<TKey, List<TInput>> _stateStore;
        private IOperator _nextOperator;

        public GroupByKeyOperator(Func<TInput, TKey> keySelector, IStateStore<TKey, List<TInput>> stateStore)
        {
            _keySelector = keySelector;
            _stateStore = stateStore;
        }

        public IEnumerable<IStateStore> GetStateStores()
        {
            yield return _stateStore;
        }

        public void Process(object input)
        {
            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);

            // Retrieve or create the group list atomically
            List<TInput> group;
            lock (_stateStore)
            {
                group = _stateStore.Get(key) ?? new List<TInput>();
                group.Add(typedInput);
                _stateStore.Put(key, group);
            }

            // we should not return the value from the state, continue the process further, state is just used to mutate
            // for now we are commenting the next Operator.
            // _nextOperator?.Process(new KeyValuePair<TKey, List<TInput>>(key, group));

            _nextOperator?.Process(typedInput);

        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
