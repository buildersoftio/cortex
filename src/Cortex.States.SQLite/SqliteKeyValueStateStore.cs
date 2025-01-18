using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.States.Sqlite
{
    public class SqliteKeyValueStateStore<TKey, TValue> : IDataStore<TKey, TValue>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly Func<TKey, string> _keySerializer;
        private readonly Func<TValue, string> _valueSerializer;
        private readonly Func<string, TKey> _keyDeserializer;
        private readonly Func<string, TValue> _valueDeserializer;

        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private volatile bool _isInitialized;

        public string Name { get; }

        /// <summary>
        /// Initializes a new instance of the SqliteKeyValueStateStore.
        /// </summary>
        /// <param name="name">A friendly name for the store.</param>
        /// <param name="connectionString">
        ///     The connection string to the SQLite database.
        ///     For example, "Data Source=MyDatabase.db" or an in-memory DB "Data Source=:memory:".
        /// </param>
        /// <param name="tableName">The name of the table to use for storing state entries.</param>
        /// <param name="keySerializer">Optional key serializer. If not provided, JSON serialization is used.</param>
        /// <param name="valueSerializer">Optional value serializer. If not provided, JSON serialization is used.</param>
        /// <param name="keyDeserializer">Optional key deserializer. If not provided, JSON deserialization is used.</param>
        /// <param name="valueDeserializer">Optional value deserializer. If not provided, JSON deserialization is used.</param>
        public SqliteKeyValueStateStore(
            string name,
            string connectionString,
            string tableName,
            Func<TKey, string> keySerializer = null,
            Func<TValue, string> valueSerializer = null,
            Func<string, TKey> keyDeserializer = null,
            Func<string, TValue> valueDeserializer = null)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            Name = name;
            _connectionString = connectionString;
            _tableName = tableName;

            // Assign custom or default (JSON-based) serializers/deserializers
            _keySerializer = keySerializer ?? (key => JsonSerializer.Serialize(key));
            _valueSerializer = valueSerializer ?? (value => JsonSerializer.Serialize(value));
            _keyDeserializer = keyDeserializer ?? (str => JsonSerializer.Deserialize<TKey>(str)!);
            _valueDeserializer = valueDeserializer ?? (str => JsonSerializer.Deserialize<TValue>(str)!);

            // Initialize the table
            InitializeAsync().GetAwaiter().GetResult();
        }

        private async Task InitializeAsync()
        {
            if (_isInitialized) return;

            await _initializationLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_isInitialized) return;

                using (var connection = new SqliteConnection(_connectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Create the table if it does not exist
                    var createTableSql = $@"
                        CREATE TABLE IF NOT EXISTS [{_tableName}] (
                            [key] TEXT NOT NULL PRIMARY KEY,
                            [value] TEXT
                        );";

                    using (var cmd = new SqliteCommand(createTableSql, connection))
                    {
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }

                _isInitialized = true;
            }
            finally
            {
                _initializationLock.Release();
            }
        }

        private void EnsureInitialized()
        {
            if (!_isInitialized)
            {
                throw new InvalidOperationException("SqliteKeyValueStateStore is not properly initialized.");
            }
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            var sql = $@"SELECT [value] FROM [{_tableName}] WHERE [key] = @key;";
            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);

                connection.Open();
                var result = cmd.ExecuteScalar() as string;
                if (result == null)
                    return default;
                return _valueDeserializer(result);
            }
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var serializedValue = _valueSerializer(value);

            // In SQLite, a common "upsert" pattern is using INSERT OR REPLACE (or INSERT ON CONFLICT).
            var sql = $@"
                INSERT OR REPLACE INTO [{_tableName}] ([key], [value])
                VALUES (@key, @value);";

            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@value", (object)serializedValue ?? DBNull.Value);

                connection.Open();
                cmd.ExecuteNonQuery();
            }
        }

        public bool ContainsKey(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            var sql = $@"SELECT COUNT(*) FROM [{_tableName}] WHERE [key] = @key;";
            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);

                connection.Open();
                var count = (long)cmd.ExecuteScalar()!; // In SQLite, COUNT returns long
                return count > 0;
            }
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            var sql = $@"DELETE FROM [{_tableName}] WHERE [key] = @key;";
            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);

                connection.Open();
                cmd.ExecuteNonQuery();
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            EnsureInitialized();

            var sql = $@"SELECT [key], [value] FROM [{_tableName}];";
            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                connection.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var serializedKey = reader.GetString(0);
                        var serializedValue = reader.IsDBNull(1) ? null : reader.GetString(1);

                        var key = _keyDeserializer(serializedKey);
                        var value = serializedValue == null ? default : _valueDeserializer(serializedValue);

                        yield return new KeyValuePair<TKey, TValue>(key, value!);
                    }
                }
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            EnsureInitialized();

            var sql = $@"SELECT [key] FROM [{_tableName}];";
            using (var connection = new SqliteConnection(_connectionString))
            using (var cmd = new SqliteCommand(sql, connection))
            {
                connection.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var serializedKey = reader.GetString(0);
                        yield return _keyDeserializer(serializedKey);
                    }
                }
            }
        }

        public void Dispose()
        {
            // Nothing special to dispose in this implementation.
            _initializationLock.Dispose();
        }
    }
}
