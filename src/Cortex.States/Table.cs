using Cortex.States.Abstractions;
using System.Collections.Generic;

namespace Cortex.States
{
    public class Table<TKey, TValue> : ITable<TKey, TValue>
    {
        private readonly IDataStore<TKey, TValue> _dataStore;

        public Table(IDataStore<TKey, TValue> dataStore)
        {
            _dataStore = dataStore;
        }

        public TValue Get(TKey key) => _dataStore.Get(key);
        public bool ContainsKey(TKey key) => _dataStore.ContainsKey(key);
        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll() => _dataStore.GetAll();
        public IEnumerable<TKey> GetKeys() => _dataStore.GetKeys();
    }
}
