using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.States.MSSqlServer
{
    public class SqlServerKeyValueStateStore<TKey, TValue> : IDataStore<TKey, TValue>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _tableName;
        private readonly Func<TKey, string> _keySerializer;
        private readonly Func<TValue, string> _valueSerializer;
        private readonly Func<string, TKey> _keyDeserializer;
        private readonly Func<string, TValue> _valueDeserializer;

        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private volatile bool _isInitialized;

        public string Name { get; }

        /// <summary>
        /// Initializes a new instance of the SqlServerStateStore.
        /// </summary>
        /// <param name="name">A friendly name for the store.</param>
        /// <param name="connectionString">The connection string to the SQL Server database.</param>
        /// <param name="schemaName">The schema name under which the table will be created. Defaults to "dbo".</param>
        /// <param name="tableName">The name of the table to use for storing state entries.</param>
        /// <param name="keySerializer">Optional key serializer. If not provided, JSON serialization is used.</param>
        /// <param name="valueSerializer">Optional value serializer. If not provided, JSON serialization is used.</param>
        /// <param name="keyDeserializer">Optional key deserializer. If not provided, JSON deserialization is used.</param>
        /// <param name="valueDeserializer">Optional value deserializer. If not provided, JSON deserialization is used.</param>
        public SqlServerKeyValueStateStore(
            string name,
            string connectionString,
            string tableName,
            string schemaName = "dbo",
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
            _schemaName = schemaName;
            _tableName = tableName;

            _keySerializer = key => JsonSerializer.Serialize(key);
            _valueSerializer = value => JsonSerializer.Serialize(value);
            _keyDeserializer = str => JsonSerializer.Deserialize<TKey>(str)!;
            _valueDeserializer = str => JsonSerializer.Deserialize<TValue>(str)!;

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

                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Ensure schema exists (if not dbo)
                    if (!string.Equals(_schemaName, "dbo", StringComparison.OrdinalIgnoreCase))
                    {
                        var createSchemaSql = $@"
                            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schemaName}')
                            BEGIN
                                EXEC('CREATE SCHEMA [{_schemaName}]')
                            END";
                        using (var cmd = new SqlCommand(createSchemaSql, connection))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }

                    // Ensure table exists
                    var createTableSql = $@"
                        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_schemaName}' AND TABLE_NAME = '{_tableName}')
                        BEGIN
                            CREATE TABLE [{_schemaName}].[{_tableName}] (
                                [key] NVARCHAR(450) NOT NULL PRIMARY KEY,
                                [value] NVARCHAR(MAX) NULL
                            );
                        END";
                    using (var cmd = new SqlCommand(createTableSql, connection))
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
                throw new InvalidOperationException("SqlServerStateStore is not properly initialized.");
            }
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            var sql = $@"SELECT [value] FROM [{_schemaName}].[{_tableName}] WHERE [key] = @key";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
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

            // Upsert pattern using MERGE or a simple IF EXISTS
            var sql = $@"
                IF EXISTS (SELECT 1 FROM [{_schemaName}].[{_tableName}] WHERE [key] = @key)
                BEGIN
                    UPDATE [{_schemaName}].[{_tableName}] SET [value] = @value WHERE [key] = @key;
                END
                ELSE
                BEGIN
                    INSERT INTO [{_schemaName}].[{_tableName}] ([key], [value]) VALUES (@key, @value);
                END";

            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
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

            var sql = $@"SELECT COUNT(*) FROM [{_schemaName}].[{_tableName}] WHERE [key] = @key";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);
                connection.Open();
                var count = (int)cmd.ExecuteScalar();
                return count > 0;
            }
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();

            var serializedKey = _keySerializer(key);

            var sql = $@"DELETE FROM [{_schemaName}].[{_tableName}] WHERE [key] = @key";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", (object)serializedKey ?? DBNull.Value);
                connection.Open();
                cmd.ExecuteNonQuery();
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            EnsureInitialized();

            var sql = $@"SELECT [key], [value] FROM [{_schemaName}].[{_tableName}]";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
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

            var sql = $@"SELECT [key] FROM [{_schemaName}].[{_tableName}]";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
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
            // Nothing to dispose specifically here since we create new connections on each call.
            // Just in case, we can attempt to release the initialization lock.
            _initializationLock.Dispose();
        }

    }
}
