using Npgsql;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.States.PostgreSQL
{
    public class PostgresKeyValueStateStore<TKey, TValue> : IDataStore<TKey, TValue>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _tableName;

        private readonly Func<TKey, string> _keySerializer;
        private readonly Func<TValue, string> _valueSerializer;
        private readonly Func<string, TKey> _keyDeserializer;
        private readonly Func<string, TValue> _valueDeserializer;

        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private bool _isInitialized = false;

        public string Name { get; }

        /// <summary>
        /// Creates a new PostgresStateStore.
        /// </summary>
        /// <param name="name">A friendly name for the state store.</param>
        /// <param name="connectionString">The Postgres connection string.</param>
        /// <param name="schemaName">The schema where the table will be created.</param>
        /// <param name="tableName">The table name to store key-value pairs.</param>
        public PostgresKeyValueStateStore(
            string name,
            string connectionString,
            string tableName,
            string schemaName = "public")
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _schemaName = string.IsNullOrWhiteSpace(schemaName) ? "public" : schemaName;
            _tableName = string.IsNullOrWhiteSpace(tableName) ? "state_store" : tableName;

            // Default JSON-based (de)serialization
            _keySerializer = key => JsonSerializer.Serialize(key);
            _valueSerializer = value => JsonSerializer.Serialize(value);
            _keyDeserializer = str => JsonSerializer.Deserialize<TKey>(str);
            _valueDeserializer = str => JsonSerializer.Deserialize<TValue>(str);

            // Initialize the schema/table
            Initialize().GetAwaiter().GetResult();
        }

        private async Task Initialize()
        {
            if (_isInitialized) return;

            await _initializationLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_isInitialized) return;

                using var conn = new NpgsqlConnection(_connectionString);
                await conn.OpenAsync().ConfigureAwait(false);

                // Create schema if not exists
                var createSchemaCmd = new NpgsqlCommand($@"
                    CREATE SCHEMA IF NOT EXISTS ""{_schemaName}"";
                ", conn);

                await createSchemaCmd.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Create table if it doesn't exist
                var createTableCmd = new NpgsqlCommand($@"
                    CREATE TABLE IF NOT EXISTS ""{_schemaName}"".""{_tableName}"" (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    );
                ", conn);

                await createTableCmd.ExecuteNonQueryAsync().ConfigureAwait(false);

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
                throw new InvalidOperationException("PostgresStateStore is not initialized properly.");
            }
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand($@"
                SELECT value FROM ""{_schemaName}"".""{_tableName}"" WHERE key = @key;
            ", conn);

            cmd.Parameters.AddWithValue("key", serializedKey);

            var result = cmd.ExecuteScalar() as string;
            if (result == null)
                return default;

            return _valueDeserializer(result);
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);
            var serializedValue = _valueSerializer(value);

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            // Upsert logic (Postgres 9.5+)
            using var cmd = new NpgsqlCommand($@"
                INSERT INTO ""{_schemaName}"".""{_tableName}"" (key, value)
                VALUES (@key, @value)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            ", conn);

            cmd.Parameters.AddWithValue("key", serializedKey);
            cmd.Parameters.AddWithValue("value", serializedValue);

            cmd.ExecuteNonQuery();
        }

        public bool ContainsKey(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand($@"
                SELECT 1 FROM ""{_schemaName}"".""{_tableName}""
                WHERE key = @key LIMIT 1;
            ", conn);
            cmd.Parameters.AddWithValue("key", serializedKey);

            var exists = cmd.ExecuteScalar();
            return exists != null;
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand($@"
                DELETE FROM ""{_schemaName}"".""{_tableName}""
                WHERE key = @key;
            ", conn);

            cmd.Parameters.AddWithValue("key", serializedKey);
            cmd.ExecuteNonQuery();
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            EnsureInitialized();

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand($@"
                SELECT key, value FROM ""{_schemaName}"".""{_tableName}"";
            ", conn);

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var serializedKey = reader.GetString(0);
                var serializedValue = reader.GetString(1);

                var key = _keyDeserializer(serializedKey);
                var value = _valueDeserializer(serializedValue);

                yield return new KeyValuePair<TKey, TValue>(key, value);
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            EnsureInitialized();

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand($@"
                SELECT key FROM ""{_schemaName}"".""{_tableName}""; 
            ", conn);

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var serializedKey = reader.GetString(0);
                yield return _keyDeserializer(serializedKey);
            }
        }

        public void Dispose()
        {
            // Since we create a new connection per operation, there's no connection-level resource to dispose.
            // The semaphore does not need disposal as we share it statically. If needed, could be disposed at application end.
        }
    }
}