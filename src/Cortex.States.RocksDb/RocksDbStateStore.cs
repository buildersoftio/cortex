using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Xml.Linq;

namespace Cortex.States.RocksDb
{
    public class RocksDbStateStore<TKey, TValue> : IStateStore<TKey, TValue>
    {
        private readonly RocksDbSharp.RocksDb _db;
        private readonly ColumnFamilyHandle _handle;
        private readonly Func<TKey, byte[]> _keySerializer;
        private readonly Func<TValue, byte[]> _valueSerializer;
        private readonly Func<byte[], TKey> _keyDeserializer;
        private readonly Func<byte[], TValue> _valueDeserializer;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
        public string Name { get; }

        public RocksDbStateStore(string name, string dbPath)
        {
            Name = name;
            var options = new DbOptions().SetCreateIfMissing(true);
            _db = RocksDbSharp.RocksDb.Open(options, dbPath);

            // Default serializers using JSON
            _keySerializer = key => JsonSerializer.SerializeToUtf8Bytes(key);
            _valueSerializer = value => JsonSerializer.SerializeToUtf8Bytes(value);
            _keyDeserializer = bytes => JsonSerializer.Deserialize<TKey>(bytes);
            _valueDeserializer = bytes => JsonSerializer.Deserialize<TValue>(bytes);
        }

        public TValue Get(TKey key)
        {
            _lock.EnterReadLock();
            try
            {
                var keyBytes = _keySerializer(key);
                var valueBytes = _db.Get(keyBytes);
                if (valueBytes == null)
                    return default;

                return _valueDeserializer(valueBytes);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void Put(TKey key, TValue value)
        {
            _lock.EnterWriteLock();
            try
            {
                var keyBytes = _keySerializer(key);
                var valueBytes = _valueSerializer(value);
                _db.Put(keyBytes, valueBytes);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool ContainsKey(TKey key)
        {
            _lock.EnterReadLock();
            try
            {
                var keyBytes = _keySerializer(key);
                var valueBytes = _db.Get(keyBytes);
                return valueBytes != null;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void Remove(TKey key)
        {
            _lock.EnterWriteLock();
            try
            {
                var keyBytes = _keySerializer(key);
                _db.Remove(keyBytes);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            _lock.EnterReadLock();
            try
            {
                using (var iterator = _db.NewIterator())
                {
                    iterator.SeekToFirst();
                    while (iterator.Valid())
                    {
                        var key = _keyDeserializer(iterator.Key());
                        var value = _valueDeserializer(iterator.Value());
                        yield return new KeyValuePair<TKey, TValue>(key, value);
                        iterator.Next();
                    }
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void Dispose()
        {
            _db?.Dispose();
            _lock?.Dispose();
        }
    }
}
