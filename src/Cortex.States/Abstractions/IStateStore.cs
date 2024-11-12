using System.Collections.Generic;

namespace Cortex.States
{
    public interface IStateStore<TKey, TValue>
    {
        TValue Get(TKey key);
        void Put(TKey key, TValue value);
        bool ContainsKey(TKey key);
        void Remove(TKey key);
        IEnumerable<KeyValuePair<TKey, TValue>> GetAll();
    }
}
