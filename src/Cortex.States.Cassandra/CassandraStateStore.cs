using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.States.Cassandra
{
    public class CassandraStateStore<TKey, TValue> : IStateStore<TKey, TValue>
    {
        private readonly ISession _session;
        private readonly string _keyspace;
        private readonly string _tableName;
        private readonly PreparedStatement _getStatement;
        private readonly PreparedStatement _putStatement;
        private readonly PreparedStatement _removeStatement;
        private readonly PreparedStatement _getAllStatement;
        private readonly PreparedStatement _getKeysStatement;
        private readonly Func<TKey, string> _keySerializer;
        private readonly Func<TValue, string> _valueSerializer;
        private readonly Func<string, TKey> _keyDeserializer;
        private readonly Func<string, TValue> _valueDeserializer;


        // SemaphoreSlim for initialization synchronization
        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);

        // Flag to track initialization status
        private volatile bool _isInitialized;

        // Cancellation token source for cleanup
        private readonly CancellationTokenSource _cancellationTokenSource;

        public string Name { get; }


        /// <summary>
        /// Initializes a new instance of the CassandraStateStore.
        /// </summary>
        /// <param name="name">Name of the state store</param>
        /// <param name="keyspace">Keyspace name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="session">Cassandra session</param>
        /// <param name="keyspaceConfig">Optional keyspace configuration</param>
        /// <param name="readConsistency">Read consistency level</param>
        /// <param name="writeConsistency">Write consistency level</param>
        public CassandraStateStore(
           string name,
           string keyspace,
           string tableName,
           ISession session,
           KeyspaceConfiguration keyspaceConfig = null,
           ConsistencyLevel? readConsistency = null,
           ConsistencyLevel? writeConsistency = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _keyspace = keyspace ?? throw new ArgumentNullException(nameof(keyspace));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _cancellationTokenSource = new CancellationTokenSource();

            // Initialize with the provided configuration or default
            InitializeAsync(keyspaceConfig ?? new KeyspaceConfiguration()).GetAwaiter().GetResult();

            // Prepare statements with specified consistency levels
            _getStatement = _session.Prepare(
                $"SELECT value FROM {_keyspace}.{_tableName} WHERE key = ?")
                .SetConsistencyLevel(readConsistency ?? ConsistencyLevel.Quorum);

            _putStatement = _session.Prepare(
                $"INSERT INTO {_keyspace}.{_tableName} (key, value) VALUES (?, ?)")
                .SetConsistencyLevel(writeConsistency ?? ConsistencyLevel.Quorum);

            _removeStatement = _session.Prepare(
                $"DELETE FROM {_keyspace}.{_tableName} WHERE key = ?")
                .SetConsistencyLevel(writeConsistency ?? ConsistencyLevel.Quorum);

            _getAllStatement = _session.Prepare(
                $"SELECT key, value FROM {_keyspace}.{_tableName}")
                .SetConsistencyLevel(readConsistency ?? ConsistencyLevel.Quorum);

            _getKeysStatement = _session.Prepare(
                $"SELECT key FROM {_keyspace}.{_tableName}")
                .SetConsistencyLevel(readConsistency ?? ConsistencyLevel.Quorum);

            _keySerializer = key => JsonSerializer.Serialize(key);
            _valueSerializer = value => JsonSerializer.Serialize(value);
            _keyDeserializer = str => JsonSerializer.Deserialize<TKey>(str);
            _valueDeserializer = str => JsonSerializer.Deserialize<TValue>(str);
        }

        private async Task InitializeAsync(KeyspaceConfiguration config)
        {
            if (_isInitialized) return;

            await _initializationLock.WaitAsync();
            try
            {
                if (_isInitialized) return;

                // Create keyspace using the provided configuration
                var createKeyspaceQuery = config.GenerateCreateKeyspaceCql(_keyspace);
                await _session.ExecuteAsync(new SimpleStatement(createKeyspaceQuery));

                // Create table if it doesn't exist
                await _session.ExecuteAsync(new SimpleStatement(
                    $@"CREATE TABLE IF NOT EXISTS {_keyspace}.{_tableName} (
                        key text PRIMARY KEY,
                        value text
                    )"));

                _isInitialized = true;
            }
            finally
            {
                _initializationLock.Release();
            }
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var boundStatement = _getStatement.Bind(serializedKey);

            // Cassandra driver handles thread safety for execute operations
            var row = _session.Execute(boundStatement).FirstOrDefault();

            if (row == null)
                return default;

            var serializedValue = row.GetValue<string>("value");
            return _valueDeserializer(serializedValue);
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var serializedValue = _valueSerializer(value);
            var boundStatement = _putStatement.Bind(serializedKey, serializedValue);

            _session.Execute(boundStatement);
        }

        public bool ContainsKey(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var boundStatement = _getStatement.Bind(serializedKey);
            var row = _session.Execute(boundStatement).FirstOrDefault();
            return row != null;
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var boundStatement = _removeStatement.Bind(serializedKey);
            _session.Execute(boundStatement);
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            EnsureInitialized();

            var boundStatement = _getAllStatement.Bind();

            // Execute and materialize results to avoid timeout issues during enumeration
            var rows = _session.Execute(boundStatement).ToList();

            foreach (var row in rows)
            {
                var serializedKey = row.GetValue<string>("key");
                var serializedValue = row.GetValue<string>("value");
                var key = _keyDeserializer(serializedKey);
                var value = _valueDeserializer(serializedValue);
                yield return new KeyValuePair<TKey, TValue>(key, value);
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            EnsureInitialized();

            var boundStatement = _getKeysStatement.Bind();

            // Execute and materialize results to avoid timeout issues during enumeration
            var rows = _session.Execute(boundStatement).ToList();

            foreach (var row in rows)
            {
                var serializedKey = row.GetValue<string>("key");
                yield return _keyDeserializer(serializedKey);
            }
        }

        private void EnsureInitialized()
        {
            if (!_isInitialized)
            {
                throw new InvalidOperationException("CassandraStateStore is not properly initialized.");
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            _initializationLock.Dispose();
        }
    }
}
