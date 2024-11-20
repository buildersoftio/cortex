using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Cortex.States
{
    public class InMemoryStateStore<TKey, TValue> : IStateStore<TKey, TValue>
    {
        private readonly ConcurrentDictionary<TKey, TValue> _store = new ConcurrentDictionary<TKey, TValue>();

        public string Name { get; }

        public InMemoryStateStore(string name)
        {
            Name = name;
        }

        public TValue Get(TKey key)
        {
            if (_store.TryGetValue(key, out var value))
                return value;

            return default;
        }

        public void Put(TKey key, TValue value)
        {
            _store[key] = value;
        }

        public bool ContainsKey(TKey key)
        {
            return _store.ContainsKey(key);
        }

        public void Remove(TKey key)
        {
            _store.TryRemove(key, out _);
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            return _store;
        }
    }
}
