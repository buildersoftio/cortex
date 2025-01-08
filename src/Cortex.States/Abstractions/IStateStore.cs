using System.Collections.Generic;

namespace Cortex.States
{
    public interface IDataStore
    {
        string Name { get; }
    }

    public interface IDataStore<TKey, TValue> : IDataStore
    {
        TValue Get(TKey key);
        void Put(TKey key, TValue value);
        bool ContainsKey(TKey key);
        void Remove(TKey key);
        IEnumerable<KeyValuePair<TKey, TValue>> GetAll();
        IEnumerable<TKey> GetKeys();
    }
}
