using System;
using System.Collections.Generic;

namespace Cortex.Streams.Windows
{
    /// <summary>
    /// Represents a composite key for session results.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    public class SessionKey<TKey>
    {
        public TKey Key { get; set; }
        public DateTime SessionStartTime { get; set; }
        public DateTime SessionEndTime { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is SessionKey<TKey> other)
            {
                return EqualityComparer<TKey>.Default.Equals(Key, other.Key)
                    && SessionStartTime.Equals(other.SessionStartTime)
                    && SessionEndTime.Equals(other.SessionEndTime);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int hashKey = Key != null ? Key.GetHashCode() : 0;
            int hashStartTime = SessionStartTime.GetHashCode();
            int hashEndTime = SessionEndTime.GetHashCode();
            return hashKey ^ hashStartTime ^ hashEndTime;
        }
    }
}
