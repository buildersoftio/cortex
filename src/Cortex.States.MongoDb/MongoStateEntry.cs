using MongoDB.Bson.Serialization.Attributes;

namespace Cortex.States.MongoDb
{
    /// <summary>
    /// Represents a single key-value entry in MongoDB.
    /// The _id field will serve as the key, ensuring uniqueness and indexing.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class MongoStateEntry<TKey, TValue>
    {
        [BsonId]
        public TKey Id { get; set; }

        [BsonElement("value")]
        public TValue Value { get; set; }
    }
}
