using System;
using System.Collections.Generic;

namespace Cortex.Streams.Windows
{

    /// <summary>
    /// Represents a composite key for window results.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    public class WindowKey<TKey>
    {
        public TKey Key { get; set; }
        public DateTime WindowStartTime { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is WindowKey<TKey> other)
            {
                return EqualityComparer<TKey>.Default.Equals(Key, other.Key)
                    && WindowStartTime.Equals(other.WindowStartTime);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int hashKey = Key != null ? Key.GetHashCode() : 0;
            int hashTime = WindowStartTime.GetHashCode();
            return hashKey ^ hashTime;
        }
    }
}
