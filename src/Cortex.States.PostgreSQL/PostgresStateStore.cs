using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Cortex.States.PostgreSQL
{
    public class PostgresStateStore<TKey, TValue> : IDataStore<TKey, TValue>, IDisposable where TValue : new()
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _baseTableName;
        private readonly bool _createOrUpdateTableSchema;

        public string Name { get; }

        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private volatile bool _isInitialized;

        private readonly PostgresTypeAnalyzer _typeAnalyzer;
        private readonly PostgresPropertyConverter _propertyConverter;
        private readonly PostgresSchemaManager _schemaManager;

        public PostgresStateStore(
            string name,
            string connectionString,
            string tableName,
            string schemaName = "public",
            bool createOrUpdateTableSchema = true)
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
            _baseTableName = tableName;
            _createOrUpdateTableSchema = createOrUpdateTableSchema;

            _typeAnalyzer = new PostgresTypeAnalyzer(typeof(TValue), _baseTableName);
            _propertyConverter = new PostgresPropertyConverter();
            _schemaManager = new PostgresSchemaManager(_connectionString, _schemaName, _baseTableName, _typeAnalyzer, _createOrUpdateTableSchema);

            Initialize();
        }

        private void Initialize()
        {
            if (_isInitialized) return;
            _initializationLock.Wait();
            try
            {
                if (_isInitialized) return;
                _schemaManager.EnsureSchemaAndTables();
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
                throw new InvalidOperationException("PostgresStateStore is not properly initialized.");
        }

        public TValue Get(TKey key)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var mainSql = $@"SELECT ""key"" FROM ""{_schemaName}"".""{_baseTableName}"" WHERE ""key"" = @key";
                    bool exists;
                    using (var cmd = new NpgsqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        var res = cmd.ExecuteScalar();
                        exists = (res != null);
                    }

                    if (!exists)
                        return default;

                    var listValue = Activator.CreateInstance<TValue>();
                    var listAsIList = (System.Collections.IList)listValue;
                    var childSql = $@"SELECT * FROM ""{_schemaName}"".""{_baseTableName}_Child"" WHERE ""key"" = @key ORDER BY ""ItemIndex""";

                    using (var cmd = new NpgsqlCommand(childSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var childInstance = Activator.CreateInstance(_typeAnalyzer.ChildItemType);
                                foreach (var cprop in _typeAnalyzer.ChildScalarProperties)
                                {
                                    if (!ColumnExists(reader, cprop.Name)) continue;
                                    var val = ReadValueFromReader(reader, cprop);
                                    cprop.SetValue(childInstance, val);
                                }
                                listAsIList.Add(childInstance);
                            }
                        }
                    }

                    return listValue;
                }
                else
                {
                    var mainSql = $@"SELECT * FROM ""{_schemaName}"".""{_baseTableName}"" WHERE ""key"" = @key";
                    TValue instance = new TValue();
                    using (var cmd = new NpgsqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                                return default;

                            foreach (var prop in _typeAnalyzer.ScalarProperties)
                            {
                                if (!ColumnExists(reader, prop.Name)) continue;
                                var val = ReadValueFromReader(reader, prop);
                                prop.SetValue(instance, val);
                            }
                        }
                    }

                    // Load list properties
                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        var listInstance = (System.Collections.IList)Activator.CreateInstance(lp.Property.PropertyType);
                        var childSql = $@"SELECT * FROM ""{_schemaName}"".""{lp.TableName}"" WHERE ""key"" = @key ORDER BY ""ItemIndex""";
                        using (var cmd = new NpgsqlCommand(childSql, connection))
                        {
                            cmd.Parameters.AddWithValue("key", serializedKey);
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var childInstance = Activator.CreateInstance(lp.ChildItemType);
                                    foreach (var cprop in lp.ChildScalarProperties)
                                    {
                                        if (!ColumnExists(reader, cprop.Name)) continue;
                                        var val = ReadValueFromReader(reader, cprop);
                                        cprop.SetValue(childInstance, val);
                                    }
                                    listInstance.Add(childInstance);
                                }
                            }
                        }

                        lp.Property.SetValue(instance, listInstance);
                    }

                    return instance;
                }
            }
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();

            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var upsertMainSql = _schemaManager.BuildUpsertMainSqlForListType();
                    using (var cmd = new NpgsqlCommand(upsertMainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }

                    var deleteChildSql = $@"DELETE FROM ""{_schemaName}"".""{_baseTableName}_Child"" WHERE ""key"" = @key";
                    using (var deleteCmd = new NpgsqlCommand(deleteChildSql, connection))
                    {
                        deleteCmd.Parameters.AddWithValue("key", serializedKey);
                        deleteCmd.ExecuteNonQuery();
                    }

                    var listAsIList = (System.Collections.IList)value;
                    var insertChildSql = _schemaManager.BuildInsertChildSqlForListType();
                    using (var insertCmd = new NpgsqlCommand(insertChildSql, connection))
                    {
                        int index = 0;
                        foreach (var item in listAsIList)
                        {
                            insertCmd.Parameters.Clear();
                            insertCmd.Parameters.AddWithValue("key", serializedKey);
                            insertCmd.Parameters.AddWithValue("ItemIndex", index);
                            foreach (var cprop in _typeAnalyzer.ChildScalarProperties)
                            {
                                var val = cprop.GetValue(item);
                                SetParameterForProperty(insertCmd, cprop, val);
                            }
                            insertCmd.ExecuteNonQuery();
                            index++;
                        }
                    }
                }
                else
                {
                    var upsertMainSql = _schemaManager.BuildUpsertMainSql();
                    using (var cmd = new NpgsqlCommand(upsertMainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        foreach (var prop in _typeAnalyzer.ScalarProperties)
                        {
                            var propVal = prop.GetValue(value);
                            SetParameterForProperty(cmd, prop, propVal);
                        }
                        cmd.ExecuteNonQuery();
                    }

                    // For each list property
                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        var deleteSql = $@"DELETE FROM ""{_schemaName}"".""{lp.TableName}"" WHERE ""key"" = @key";
                        using (var deleteCmd = new NpgsqlCommand(deleteSql, connection))
                        {
                            deleteCmd.Parameters.AddWithValue("key", serializedKey);
                            deleteCmd.ExecuteNonQuery();
                        }

                        var listValue = lp.Property.GetValue(value) as System.Collections.IEnumerable;
                        if (listValue != null)
                        {
                            int index = 0;
                            var insertSql = _schemaManager.BuildInsertChildSql(lp);
                            using (var insertCmd = new NpgsqlCommand(insertSql, connection))
                            {
                                foreach (var item in listValue)
                                {
                                    insertCmd.Parameters.Clear();
                                    insertCmd.Parameters.AddWithValue("key", serializedKey);
                                    insertCmd.Parameters.AddWithValue("ItemIndex", index);
                                    foreach (var cprop in lp.ChildScalarProperties)
                                    {
                                        var val = cprop.GetValue(item);
                                        SetParameterForProperty(insertCmd, cprop, val);
                                    }
                                    insertCmd.ExecuteNonQuery();
                                    index++;
                                }
                            }
                        }
                    }
                }
            }
        }

        public bool ContainsKey(TKey key)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();
            var sql = $@"SELECT COUNT(*) FROM ""{_schemaName}"".""{_baseTableName}"" WHERE ""key"" = @key";
            using (var connection = new NpgsqlConnection(_connectionString))
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("key", serializedKey);
                connection.Open();
                var count = (long)cmd.ExecuteScalar();
                return count > 0;
            }
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var deleteChildSql = $@"DELETE FROM ""{_schemaName}"".""{_baseTableName}_Child"" WHERE ""key"" = @key";
                    using (var cmd = new NpgsqlCommand(deleteChildSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }

                    var mainSql = $@"DELETE FROM ""{_schemaName}"".""{_baseTableName}"" WHERE ""key"" = @key";
                    using (var cmd = new NpgsqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    // Remove from child tables
                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        var deleteChildSql = $@"DELETE FROM ""{_schemaName}"".""{lp.TableName}"" WHERE ""key"" = @key";
                        using (var cmd = new NpgsqlCommand(deleteChildSql, connection))
                        {
                            cmd.Parameters.AddWithValue("key", serializedKey);
                            cmd.ExecuteNonQuery();
                        }
                    }

                    var mainSql = $@"DELETE FROM ""{_schemaName}"".""{_baseTableName}"" WHERE ""key"" = @key";
                    using (var cmd = new NpgsqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            EnsureInitialized();
            var keys = GetKeys().ToList();
            foreach (var k in keys)
            {
                yield return new KeyValuePair<TKey, TValue>(k, Get(k));
            }
        }

        public IEnumerable<TKey> GetKeys()
        {
            EnsureInitialized();
            var sql = $@"SELECT ""key"" FROM ""{_schemaName}"".""{_baseTableName}""";
            using (var connection = new NpgsqlConnection(_connectionString))
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                connection.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    var keyType = typeof(TKey);
                    while (reader.Read())
                    {
                        var keyStr = reader.GetString(0);
                        yield return (TKey)Convert.ChangeType(keyStr, keyType);
                    }
                }
            }
        }

        public void Dispose()
        {
            _initializationLock.Dispose();
        }

        #region Helper Methods

        private bool ColumnExists(NpgsqlDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        private object ReadValueFromReader(NpgsqlDataReader reader, PropertyInfo prop)
        {
            var index = reader.GetOrdinal(prop.Name);
            if (reader.IsDBNull(index)) return null;

            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
                return reader.GetInt32(index);
            if (type == typeof(long))
                return reader.GetInt64(index);
            if (type == typeof(bool))
                return reader.GetBoolean(index);
            if (type == typeof(DateTime))
                return reader.GetDateTime(index);
            if (type == typeof(decimal))
                return reader.GetDecimal(index);
            if (type == typeof(double))
                return reader.GetDouble(index);
            if (type == typeof(float))
                return (float)reader.GetDouble(index);
            if (type == typeof(Guid))
                return reader.GetGuid(index);
            if (type == typeof(TimeSpan))
                return reader.GetTimeSpan(index); // Npgsql supports TimeSpan for INTERVAL
            if (type == typeof(string))
                return reader.GetString(index);

            // fallback to string convert
            var strVal = reader.GetString(index);
            return _propertyConverter.ConvertFromString(prop.PropertyType, strVal);
        }

        private void SetParameterForProperty(NpgsqlCommand cmd, PropertyInfo prop, object propValue)
        {
            var paramName = "@" + prop.Name;
            if (!cmd.Parameters.Contains(paramName))
            {
                cmd.Parameters.Add(new NpgsqlParameter(paramName, ConvertToNpgsqlDbType(prop)) { Value = DBNull.Value });
            }

            var param = cmd.Parameters[paramName];

            if (propValue == null)
            {
                param.Value = DBNull.Value;
                return;
            }

            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            // Npgsql can handle direct assignment if we pick the right NpgsqlDbType
            if (type == typeof(TimeSpan))
            {
                param.Value = (TimeSpan)propValue; // stored as INTERVAL
            }
            else
            {
                param.Value = propValue;
            }
        }

        private NpgsqlTypes.NpgsqlDbType ConvertToNpgsqlDbType(PropertyInfo prop)
        {
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
                return NpgsqlTypes.NpgsqlDbType.Integer;
            if (type == typeof(long))
                return NpgsqlTypes.NpgsqlDbType.Bigint;
            if (type == typeof(bool))
                return NpgsqlTypes.NpgsqlDbType.Boolean;
            if (type == typeof(DateTime))
                return NpgsqlTypes.NpgsqlDbType.Timestamp;
            if (type == typeof(decimal))
                return NpgsqlTypes.NpgsqlDbType.Numeric;
            if (type == typeof(double))
                return NpgsqlTypes.NpgsqlDbType.Double;
            if (type == typeof(float))
                return NpgsqlTypes.NpgsqlDbType.Real;
            if (type == typeof(Guid))
                return NpgsqlTypes.NpgsqlDbType.Uuid;
            if (type == typeof(TimeSpan))
                return NpgsqlTypes.NpgsqlDbType.Interval;
            if (type == typeof(string))
                return NpgsqlTypes.NpgsqlDbType.Text;

            // fallback to Text
            return NpgsqlTypes.NpgsqlDbType.Text;
        }

        #endregion
    }
}
