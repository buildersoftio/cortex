using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Cortex.States.ClickHouse
{
    public class ClickHouseStateStore<TKey, TValue> : IDataStore<TKey, TValue>, IDisposable
            where TValue : new()
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly ClickHouseConfiguration _config;
        private readonly ClickHouseTypeAnalyzer _typeAnalyzer;
        private readonly ClickHousePropertyConverter _propertyConverter;
        private readonly ClickHouseSchemaManager _schemaManager;

        private static readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);
        private volatile bool _initialized;

        public string Name { get; }

        public ClickHouseStateStore(
            string name,
            string connectionString,
            string tableName,
            ClickHouseConfiguration config = null)
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
            _config = config ?? new ClickHouseConfiguration(); // default config

            _typeAnalyzer = new ClickHouseTypeAnalyzer(typeof(TValue));
            _propertyConverter = new ClickHousePropertyConverter();
            _schemaManager = new ClickHouseSchemaManager(
                _connectionString,
                _tableName,
                _config,
                _typeAnalyzer);

            Initialize();
        }

        private void Initialize()
        {
            if (_initialized) return;
            _initLock.Wait();
            try
            {
                if (_initialized) return;
                _schemaManager.EnsureSchemaAndTable();
                _initialized = true;
            }
            finally
            {
                _initLock.Release();
            }
        }

        private void EnsureInitialized()
        {
            if (!_initialized)
                throw new InvalidOperationException("ClickhouseStateStore is not initialized yet.");
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();
            var keyStr = key.ToString();

            // For ReplacingMergeTree we might want the latest row; if we always delete then insert, 
            // there's only one row per key anyway. But let's be safe in case of leftover older rows:
            string sql = $@"
SELECT *
FROM {_tableName}
WHERE key = @key
ORDER BY timestamp DESC
LIMIT 1";

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "key", Value = keyStr });

            using var reader = cmd.ExecuteReader();
            if (!reader.Read())
                return default; // not found

            var instance = new TValue();

            // Index by column name -> ordinal
            var schemaTable = reader.GetSchemaTable();
            var columnsDict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columnsDict[reader.GetName(i)] = i;
            }

            // fill scalar properties
            foreach (var prop in _typeAnalyzer.ScalarProperties)
            {
                if (!columnsDict.ContainsKey(prop.Name)) continue;
                var ordinal = columnsDict[prop.Name];
                var val = reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
                var converted = ConvertValueFromClickhouse(val, prop.PropertyType);
                prop.SetValue(instance, converted);
            }

            // fill list properties
            foreach (var prop in _typeAnalyzer.ListProperties)
            {
                if (!columnsDict.ContainsKey(prop.Name)) continue;
                var ordinal = columnsDict[prop.Name];
                if (reader.IsDBNull(ordinal))
                {
                    prop.SetValue(instance, null);
                    continue;
                }
                var arrayVal = reader.GetValue(ordinal);
                // ClickHouse client typically returns object[] or something for Array columns.
                var listInstance = Activator.CreateInstance(prop.PropertyType); // e.g. List<int>
                var addMethod = prop.PropertyType.GetMethod("Add");

                var elementType = prop.PropertyType.GetGenericArguments()[0];

                // Suppose the arrayVal is an object[] of the column's elements
                if (arrayVal is System.Collections.IEnumerable items)
                {
                    foreach (var item in items)
                    {
                        var convertedItem = ConvertValueFromClickhouse(item, elementType);
                        addMethod.Invoke(listInstance, new[] { convertedItem });
                    }
                }

                prop.SetValue(instance, listInstance);
            }

            return instance;
        }

        private object ConvertValueFromClickhouse(object val, Type targetType)
        {
            if (val == null)
                return null;

            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

            // Special case for timespan stored as ticks
            if (underlying == typeof(TimeSpan) && val is long lVal)
            {
                return TimeSpan.FromTicks(lVal);
            }

            // If target is string, just call ToString
            if (underlying == typeof(string))
                return val.ToString();

            // If we got a DateTime for a DateTime64
            if (underlying == typeof(DateTime) && val is DateTime dt)
            {
                return dt;
            }

            // if we got an integer or double etc.
            // we can just Convert.ChangeType in many cases
            // but let's do a fallback to the property converter if that fails
            try
            {
                return Convert.ChangeType(val, underlying);
            }
            catch
            {
                // fallback
                return _propertyConverter.ConvertFromString(targetType, val.ToString());
            }
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();

            // We do a "delete" then an "insert"
            Remove(key);

            var keyStr = key.ToString();

            // Build parameter lists
            var columnNames = new List<string> { "key", "timestamp" };
            var paramNames = new List<string> { "@key", "@timestamp" };
            var paramValues = new Dictionary<string, object>
            {
                { "key", keyStr },
                { "timestamp", DateTime.UtcNow } // used for ReplacingMergeTree version
            };

            // scalar properties
            foreach (var prop in _typeAnalyzer.ScalarProperties)
            {
                columnNames.Add(prop.Name);
                var pName = "@" + prop.Name;
                paramNames.Add(pName);

                var propVal = prop.GetValue(value);
                object finalVal = ConvertValueToClickhouse(propVal, prop);
                paramValues[prop.Name] = finalVal;
            }

            // list properties
            foreach (var prop in _typeAnalyzer.ListProperties)
            {
                columnNames.Add(prop.Name);
                var pName = "@" + prop.Name;
                paramNames.Add(pName);

                var propVal = prop.GetValue(value);
                // propVal might be a List<T>. We can store as object[] or typed array for Insert.
                object finalVal = ConvertListToClickhouseArray(propVal, prop);
                paramValues[prop.Name] = finalVal;
            }

            var insertSql = $@"
INSERT INTO {_tableName} 
({string.Join(",", columnNames)})
VALUES ({string.Join(",", paramNames)})";

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = insertSql;

            foreach (var kvp in paramValues)
            {
                cmd.Parameters.Add(new ClickHouseDbParameter
                {
                    ParameterName = kvp.Key,
                    Value = kvp.Value ?? DBNull.Value
                });
            }

            cmd.ExecuteNonQuery();
        }

        private object ConvertValueToClickhouse(object value, PropertyInfo prop)
        {
            if (value == null) return null;

            var underlying = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (underlying == typeof(TimeSpan))
            {
                // store as ticks
                TimeSpan ts = (TimeSpan)value;
                return ts.Ticks;
            }

            return value;
        }

        private object ConvertListToClickhouseArray(object listVal, PropertyInfo prop)
        {
            if (listVal == null) return null;
            // it's a List<T>, so let's get T
            var elementType = prop.PropertyType.GetGenericArguments()[0];
            var enumer = listVal as System.Collections.IEnumerable;
            if (enumer == null) return null;

            var result = new List<object>();
            foreach (var item in enumer)
            {
                var converted = ConvertValueFromClickhouse(item, elementType);
                // Actually we want to store "raw" form, so let's do ConvertValueToClickhouse
                // so timespan etc. become ticks
                converted = ConvertValueToClickhouse(item, prop);
                result.Add(converted);
            }
            return result.ToArray();
        }

        public bool ContainsKey(TKey key)
        {
            EnsureInitialized();
            var keyStr = key.ToString();

            var sql = $@"
SELECT count(*)
FROM {_tableName}
WHERE key = @key
";

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new ClickHouseDbParameter
            {
                ParameterName = "key",
                Value = keyStr
            });

            var count = Convert.ToInt64(cmd.ExecuteScalar());
            return count > 0;
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();

            // For a pure MergeTree, we can do an actual DELETE. This is an async operation in newer ClickHouse versions. 
            // For ReplacingMergeTree, we might do an "is_deleted=1" approach. For simplicity, let's do a DELETE if we want immediate removal.
            var keyStr = key.ToString();
            var sql = $"ALTER TABLE {_tableName} DELETE WHERE key = @key";

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new ClickHouseDbParameter
            {
                ParameterName = "key",
                Value = keyStr
            });
            cmd.ExecuteNonQuery();
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            // For large tables, you'd likely do streaming or another approach. 
            // Here we just do SELECT distinct key, then do Get(key). 
            // For a big table, that might be huge. Use with caution.
            EnsureInitialized();
            var allKeys = GetKeys().ToList();
            foreach (var k in allKeys)
            {
                yield return new KeyValuePair<TKey, TValue>(k, Get(k));
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            EnsureInitialized();

            var sql = $"SELECT distinct key FROM {_tableName}";

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();
            var keyType = typeof(TKey);

            while (reader.Read())
            {
                var keyObj = reader.GetValue(0);
                // Convert from string to TKey if needed
                yield return (TKey)Convert.ChangeType(keyObj, keyType);
            }
        }

        public void Dispose()
        {
            _initLock.Dispose();
        }
    }
}
