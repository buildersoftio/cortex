using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.States.MongoDb
{
    /// <summary>
    /// A state store implementation backed by a MongoDB collection.
    /// This implementation ensures that all operations are thread-safe and production-ready.
    /// </summary>
    /// <typeparam name="TKey">The type of the key. Must be serializable by MongoDB.</typeparam>
    /// <typeparam name="TValue">The type of the value. Must be serializable by MongoDB.</typeparam>
    public class MongoDbStateStore<TKey, TValue> : IStateStore<TKey, TValue>
    {
        private readonly IMongoCollection<MongoStateEntry<TKey, TValue>> _collection;
        public string Name { get; }

        /// <summary>
        /// Creates a new MongoDbStateStore that uses the specified MongoDB database and collection.
        /// </summary>
        /// <param name="database">The MongoDB database instance to use.</param>
        /// <param name="collectionName">The name of the collection to store key-value pairs in.</param>
        /// <param name="storeName">A friendly name for this store.</param>
        public MongoDbStateStore(string storeName, IMongoDatabase database, string collectionName)
        {
            if (database == null) throw new ArgumentNullException(nameof(database));
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name must be provided", nameof(collectionName));
            if (string.IsNullOrWhiteSpace(storeName)) throw new ArgumentException("Store name must be provided", nameof(storeName));

            Name = storeName;

            _collection = database.GetCollection<MongoStateEntry<TKey, TValue>>(collectionName);
            // Ensure collection and index. _id is indexed by default, so no special action required.
            // Additional indexes can be created here if needed.
        }

        /// <summary>
        /// Retrieves the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key to lookup.</param>
        /// <returns>The value if found; otherwise the default value of TValue.</returns>
        public TValue Get(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var filter = Builders<MongoStateEntry<TKey, TValue>>.Filter.Eq(e => e.Id, key);
            var result = _collection.Find(filter).FirstOrDefault();
            return result != null ? result.Value : default;
        }

        /// <summary>
        /// Puts the specified key-value pair into the store. If the key already exists, its value is replaced.
        /// </summary>
        /// <param name="key">The key to store.</param>
        /// <param name="value">The value to store.</param>
        public void Put(TKey key, TValue value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var filter = Builders<MongoStateEntry<TKey, TValue>>.Filter.Eq(e => e.Id, key);
            var replacement = new MongoStateEntry<TKey, TValue> { Id = key, Value = value };

            // Upsert ensures that if the key does not exist, it is created
            _collection.ReplaceOne(filter, replacement, new ReplaceOptions { IsUpsert = true });
        }

        /// <summary>
        /// Checks if the specified key exists in the store.
        /// </summary>
        /// <param name="key">The key to check for existence.</param>
        /// <returns>True if the key exists; otherwise false.</returns>
        public bool ContainsKey(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var filter = Builders<MongoStateEntry<TKey, TValue>>.Filter.Eq(e => e.Id, key);
            // Limit to 1 result for efficiency
            var count = _collection.Find(filter).Limit(1).CountDocuments();
            return count > 0;
        }

        /// <summary>
        /// Removes the value associated with the specified key, if it exists.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        public void Remove(TKey key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var filter = Builders<MongoStateEntry<TKey, TValue>>.Filter.Eq(e => e.Id, key);
            _collection.DeleteOne(filter);
        }

        /// <summary>
        /// Retrieves all key-value pairs stored.
        /// Note: This operation might be expensive if the collection is large.
        /// </summary>
        /// <returns>An IEnumerable of all key-value pairs.</returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            // For large data sets, consider using a cursor-based approach or streaming.
            // Here we just return all documents.
            var allDocs = _collection.Find(Builders<MongoStateEntry<TKey, TValue>>.Filter.Empty).ToList();
            return allDocs.Select(d => new KeyValuePair<TKey, TValue>(d.Id, d.Value));
        }

        /// <summary>
        /// Retrieves all keys stored.
        /// Note: This operation might be expensive if the collection is large.
        /// </summary>
        /// <returns>An IEnumerable of all keys.</returns>
        public IEnumerable<TKey> GetKeys()
        {
            // We can do a projection to only return the keys.
            var projection = Builders<MongoStateEntry<TKey, TValue>>.Projection.Expression(d => d.Id);
            var keys = _collection.Find(Builders<MongoStateEntry<TKey, TValue>>.Filter.Empty)
                                  .Project(projection)
                                  .ToList();
            return keys;
        }
    }
}
