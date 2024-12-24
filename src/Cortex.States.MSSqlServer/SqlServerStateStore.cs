using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Cortex.States.MSSqlServer
{
    public class SqlServerStateStore<TKey, TValue> : IStateStore<TKey, TValue>, IDisposable
            where TValue : new()
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _baseTableName;
        private readonly bool _createOrUpdateTableSchema;

        public string Name { get; }

        private static readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private volatile bool _isInitialized;

        private readonly TypeAnalyzer _typeAnalyzer;
        private readonly PropertyConverter _propertyConverter; // may be less used now
        private readonly SchemaManager _schemaManager;

        public SqlServerStateStore(
            string name,
            string connectionString,
            string tableName,
            string schemaName = "dbo",
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

            _typeAnalyzer = new TypeAnalyzer(typeof(TValue), _baseTableName);
            _propertyConverter = new PropertyConverter();
            _schemaManager = new SchemaManager(_connectionString, _schemaName, _baseTableName, _typeAnalyzer, _createOrUpdateTableSchema);

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
                throw new InvalidOperationException("SqlServerStateStore is not properly initialized.");
        }

        #region Get and Put Helpers

        private string GetSqlTypeForProperty(PropertyInfo prop)
        {
            var type = prop.PropertyType;
            var underlying = Nullable.GetUnderlyingType(type) ?? type;

            if (underlying == typeof(int) || underlying == typeof(short) || underlying == typeof(byte))
                return "INT";
            if (underlying == typeof(long))
                return "BIGINT";
            if (underlying == typeof(bool))
                return "BIT";
            if (underlying == typeof(DateTime))
                return "DATETIME2";
            if (underlying == typeof(decimal))
                return "DECIMAL(18,2)";
            if (underlying == typeof(double))
                return "FLOAT";
            if (underlying == typeof(float))
                return "REAL";
            if (underlying == typeof(Guid))
                return "UNIQUEIDENTIFIER";
            if (underlying == typeof(TimeSpan))
                return "BIGINT"; // stored as ticks
            if (underlying == typeof(string))
                return "NVARCHAR(MAX)";

            // default
            return "NVARCHAR(MAX)";
        }

        private object ReadValueFromReader(SqlDataReader reader, PropertyInfo prop)
        {
            var colIndex = reader.GetOrdinal(prop.Name);
            if (reader.IsDBNull(colIndex)) return null;

            var sqlType = GetSqlTypeForProperty(prop);
            var underlying = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            // Read based on sqlType
            switch (sqlType)
            {
                case "INT":
                    int iVal = reader.GetInt32(colIndex);
                    return ConvertToNullableIfNeeded(iVal, prop.PropertyType);
                case "BIGINT":
                    long lVal = reader.GetInt64(colIndex);
                    if (underlying == typeof(TimeSpan))
                    {
                        return ConvertToNullableIfNeeded(TimeSpan.FromTicks(lVal), prop.PropertyType);
                    }
                    return ConvertToNullableIfNeeded(lVal, prop.PropertyType);
                case "BIT":
                    bool bVal = reader.GetBoolean(colIndex);
                    return ConvertToNullableIfNeeded(bVal, prop.PropertyType);
                case "DATETIME2":
                    DateTime dtVal = reader.GetDateTime(colIndex);
                    return ConvertToNullableIfNeeded(dtVal, prop.PropertyType);
                case "DECIMAL(18,2)":
                    decimal decVal = reader.GetDecimal(colIndex);
                    return ConvertToNullableIfNeeded(decVal, prop.PropertyType);
                case "FLOAT":
                    double dVal = reader.GetDouble(colIndex);
                    return ConvertToNullableIfNeeded(dVal, prop.PropertyType);
                case "REAL":
                    float fVal = (float)reader.GetDouble(colIndex); // or reader.GetFloat if available
                    return ConvertToNullableIfNeeded(fVal, prop.PropertyType);
                case "UNIQUEIDENTIFIER":
                    Guid gVal = reader.GetGuid(colIndex);
                    return ConvertToNullableIfNeeded(gVal, prop.PropertyType);
                default:
                    // NVARCHAR(MAX) or unknown
                    // For strings or unknown, we get string
                    string sVal = reader.GetString(colIndex);
                    // if property isn't string but something else unknown, attempt convert
                    return ConvertToNullableIfNeeded(ConvertFromStringFallback(sVal, prop.PropertyType), prop.PropertyType);
            }
        }

        private object ConvertFromStringFallback(string value, Type targetType)
        {
            // fallback if needed; just try basic converter or return string as is
            if (targetType == typeof(string) || Nullable.GetUnderlyingType(targetType) == typeof(string))
                return value;

            // Attempt using propertyConverter as a fallback
            return _propertyConverter.ConvertFromString(targetType, value);
        }

        private object ConvertToNullableIfNeeded<T>(T value, Type targetType)
        {
            // If targetType is nullable and value is default, handle if needed
            // Actually no extra logic needed, just return value as object.
            return value;
        }

        private void SetParameterValueForProperty(SqlParameter param, PropertyInfo prop, object propValue)
        {
            if (propValue == null)
            {
                param.Value = DBNull.Value;
                return;
            }

            var sqlType = GetSqlTypeForProperty(prop);
            var underlying = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (underlying == typeof(TimeSpan) && sqlType == "BIGINT")
            {
                var ts = (TimeSpan)propValue;
                param.Value = ts.Ticks;
                return;
            }

            // For other types, just assign the value directly. ADO.NET will handle conversions.
            // Make sure to assign the actual typed value if needed:
            // For a decimal property and DECIMAL(18,2): param.Value = (decimal)propValue;
            // For a GUID: param.Value = (Guid)propValue;
            // For datetime: param.Value = (DateTime)propValue;
            // etc.

            param.Value = propValue;
        }

        #endregion

        public TValue Get(TKey key)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var mainSql = $"SELECT [key] FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key";
                    bool exists;
                    using (var cmd = new SqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        var res = cmd.ExecuteScalar();
                        exists = (res != null);
                    }

                    if (!exists)
                        return default;

                    var listValue = Activator.CreateInstance<TValue>();
                    var listAsIList = (System.Collections.IList)listValue;
                    var childSql = $"SELECT * FROM [{_schemaName}].[{_baseTableName}_Child] WHERE [key] = @key ORDER BY [ItemIndex]";

                    using (var cmd = new SqlCommand(childSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
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
                    var mainSql = $"SELECT * FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key";
                    TValue instance = new TValue();
                    using (var cmd = new SqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                                return default;

                            // Set scalar properties
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
                        var childSql = $"SELECT * FROM [{_schemaName}].[{lp.TableName}] WHERE [key] = @key ORDER BY [ItemIndex]";
                        using (var cmd = new SqlCommand(childSql, connection))
                        {
                            cmd.Parameters.AddWithValue("@key", serializedKey);
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

        private bool ColumnExists(SqlDataReader reader, string columnName)
        {
            try
            {
                return reader.GetOrdinal(columnName) >= 0;
            }
            catch
            {
                return false;
            }
        }

        public void Put(TKey key, TValue value)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var upsertMainSql = _schemaManager.BuildUpsertMainSqlForListType();
                    using (var cmd = new SqlCommand(upsertMainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }

                    var deleteChildSql = $"DELETE FROM [{_schemaName}].[{_baseTableName}_Child] WHERE [key] = @key";
                    using (var deleteCmd = new SqlCommand(deleteChildSql, connection))
                    {
                        deleteCmd.Parameters.AddWithValue("@key", serializedKey);
                        deleteCmd.ExecuteNonQuery();
                    }

                    var listAsIList = (System.Collections.IList)value;
                    var insertChildSql = _schemaManager.BuildInsertChildSqlForListType();
                    using (var insertCmd = new SqlCommand(insertChildSql, connection))
                    {
                        int index = 0;
                        foreach (var item in listAsIList)
                        {
                            insertCmd.Parameters.Clear();
                            insertCmd.Parameters.AddWithValue("@key", serializedKey);
                            insertCmd.Parameters.AddWithValue("@ItemIndex", index);
                            foreach (var cprop in _typeAnalyzer.ChildScalarProperties)
                            {
                                var param = insertCmd.Parameters.Add("@" + cprop.Name, System.Data.SqlDbType.VarChar); // dummy
                                var val = cprop.GetValue(item);
                                SetParameterForProperty(param, cprop, val);
                            }
                            insertCmd.ExecuteNonQuery();
                            index++;
                        }
                    }
                }
                else
                {
                    var upsertMainSql = _schemaManager.BuildUpsertMainSql();
                    using (var cmd = new SqlCommand(upsertMainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        foreach (var prop in _typeAnalyzer.ScalarProperties)
                        {
                            var param = cmd.Parameters.Add("@" + prop.Name, System.Data.SqlDbType.VarChar); // dummy type
                            var propVal = prop.GetValue(value);
                            SetParameterForProperty(param, prop, propVal);
                        }
                        cmd.ExecuteNonQuery();
                    }

                    // For each list property
                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        var deleteSql = $"DELETE FROM [{_schemaName}].[{lp.TableName}] WHERE [key] = @key";
                        using (var deleteCmd = new SqlCommand(deleteSql, connection))
                        {
                            deleteCmd.Parameters.AddWithValue("@key", serializedKey);
                            deleteCmd.ExecuteNonQuery();
                        }

                        var listValue = lp.Property.GetValue(value) as System.Collections.IEnumerable;
                        if (listValue != null)
                        {
                            int index = 0;
                            var insertSql = _schemaManager.BuildInsertChildSql(lp);
                            using (var insertCmd = new SqlCommand(insertSql, connection))
                            {
                                foreach (var item in listValue)
                                {
                                    insertCmd.Parameters.Clear();
                                    insertCmd.Parameters.AddWithValue("@key", serializedKey);
                                    insertCmd.Parameters.AddWithValue("@ItemIndex", index);
                                    foreach (var cprop in lp.ChildScalarProperties)
                                    {
                                        var param = insertCmd.Parameters.Add("@" + cprop.Name, System.Data.SqlDbType.Variant);
                                        var val = cprop.GetValue(item);
                                        SetParameterValueForProperty(param, cprop, val);
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
            var sql = $"SELECT COUNT(*) FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@key", serializedKey);
                connection.Open();
                var count = (int)cmd.ExecuteScalar();
                return count > 0;
            }
        }

        public void Remove(TKey key)
        {
            EnsureInitialized();
            var serializedKey = key.ToString();
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                if (_typeAnalyzer.IsListType)
                {
                    var deleteChildSql = $"DELETE FROM [{_schemaName}].[{_baseTableName}_Child] WHERE [key] = @key";
                    using (var cmd = new SqlCommand(deleteChildSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }

                    var mainSql = $"DELETE FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key";
                    using (var cmd = new SqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    // Remove from child tables
                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        var deleteChildSql = $"DELETE FROM [{_schemaName}].[{lp.TableName}] WHERE [key] = @key";
                        using (var cmd = new SqlCommand(deleteChildSql, connection))
                        {
                            cmd.Parameters.AddWithValue("@key", serializedKey);
                            cmd.ExecuteNonQuery();
                        }
                    }

                    var mainSql = $"DELETE FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key";
                    using (var cmd = new SqlCommand(mainSql, connection))
                    {
                        cmd.Parameters.AddWithValue("@key", serializedKey);
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
            var sql = $"SELECT [key] FROM [{_schemaName}].[{_baseTableName}]";
            using (var connection = new SqlConnection(_connectionString))
            using (var cmd = new SqlCommand(sql, connection))
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

        private SqlDbType GetSqlDbTypeForProperty(PropertyInfo prop)
        {
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
                return SqlDbType.Int;
            if (type == typeof(long))
                return SqlDbType.BigInt;
            if (type == typeof(bool))
                return SqlDbType.Bit;
            if (type == typeof(DateTime))
                return SqlDbType.DateTime2;
            if (type == typeof(decimal))
                return SqlDbType.Decimal;
            if (type == typeof(double))
                return SqlDbType.Float;
            if (type == typeof(float))
                return SqlDbType.Real;
            if (type == typeof(Guid))
                return SqlDbType.UniqueIdentifier;
            if (type == typeof(TimeSpan))
                return SqlDbType.BigInt; // storing ticks in BIGINT
            if (type == typeof(string))
                return SqlDbType.NVarChar;

            // Default to NVarChar for unknown
            return SqlDbType.NVarChar;
        }

        private void SetParameterForProperty(SqlParameter param, PropertyInfo prop, object propValue)
        {
            param.SqlDbType = GetSqlDbTypeForProperty(prop);

            // If it's a string and we said NVarChar, we can set Size = -1 for NVARCHAR(MAX)
            if (param.SqlDbType == SqlDbType.NVarChar)
            {
                param.Size = -1;
            }
            else if (param.SqlDbType == SqlDbType.Decimal)
            {
                // For DECIMAL(18,2)
                param.Precision = 18;
                param.Scale = 2;
            }

            // Handle TimeSpan as ticks if needed
            var underlying = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            if (underlying == typeof(TimeSpan) && param.SqlDbType == SqlDbType.BigInt)
            {
                if (propValue == null)
                {
                    param.Value = DBNull.Value;
                }
                else
                {
                    TimeSpan ts = (TimeSpan)propValue;
                    param.Value = ts.Ticks;
                }
                return;
            }

            // For other types, just assign directly
            if (propValue == null)
            {
                param.Value = DBNull.Value;
            }
            else
            {
                param.Value = propValue;
            }
        }

        public void Dispose()
        {
            _initializationLock.Dispose();
        }
    }
}
