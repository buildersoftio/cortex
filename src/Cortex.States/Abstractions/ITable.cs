using System.Collections.Generic;

namespace Cortex.States.Abstractions
{
    public interface ITable<TKey, TValue>
    {
        TValue Get(TKey key);
        bool ContainsKey(TKey key);
        IEnumerable<KeyValuePair<TKey, TValue>> GetAll();
        IEnumerable<TKey> GetKeys();
    }
}
