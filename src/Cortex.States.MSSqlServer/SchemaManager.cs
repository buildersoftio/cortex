using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Cortex.States.MSSqlServer
{
    internal class SchemaManager
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _baseTableName;
        private readonly TypeAnalyzer _typeAnalyzer;
        private readonly bool _createOrUpdateTableSchema;

        public SchemaManager(string connectionString, string schemaName, string baseTableName, TypeAnalyzer typeAnalyzer, bool createOrUpdateTableSchema)
        {
            _connectionString = connectionString;
            _schemaName = schemaName;
            _baseTableName = baseTableName;
            _typeAnalyzer = typeAnalyzer;
            _createOrUpdateTableSchema = createOrUpdateTableSchema;
        }

        public void EnsureSchemaAndTables()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                EnsureSchema(connection);

                if (_typeAnalyzer.IsListType)
                {
                    EnsureMainTableForListType(connection);
                    EnsureChildTableForListType(connection);
                }
                else
                {
                    EnsureMainTable(connection);

                    foreach (var lp in _typeAnalyzer.ListProperties)
                    {
                        EnsureChildTable(connection, lp);
                    }
                }
            }
        }

        private void EnsureSchema(SqlConnection connection)
        {
            if (_schemaName.ToLower() != "dbo")
            {
                var createSchemaSql = $@"
                    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schemaName}')
                    BEGIN
                        EXEC('CREATE SCHEMA [{_schemaName}]')
                    END";
                using (var cmd = new SqlCommand(createSchemaSql, connection))
                {
                    if (_createOrUpdateTableSchema)
                    {
                        cmd.ExecuteNonQuery();
                    }
                    else
                    {
                        var checkSchemaSql = $"SELECT 1 FROM sys.schemas WHERE name = '{_schemaName}'";
                        using (var checkCmd = new SqlCommand(checkSchemaSql, connection))
                        {
                            var exists = checkCmd.ExecuteScalar();
                            if (exists == null) throw new InvalidOperationException($"Schema {_schemaName} does not exist and createOrUpdateTableSchema=false.");
                        }
                    }
                }
            }
        }

        private void EnsureMainTable(SqlConnection connection)
        {
            if (!TableExists(connection, _baseTableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var mainTableSql = BuildCreateMainTableSql();
                    using (var cmd = new SqlCommand(mainTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Main table [{_schemaName}].[{_baseTableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                // Check columns
                var propMap = _typeAnalyzer.ScalarProperties.ToDictionary(p => p.Name, p => p);
                EnsureColumns(connection, _baseTableName, propMap, isChildTable: false, isListType: false);
            }
        }

        private void EnsureMainTableForListType(SqlConnection connection)
        {
            if (!TableExists(connection, _baseTableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var sql = $@"CREATE TABLE [{_schemaName}].[{_baseTableName}] (
                                 [key] NVARCHAR(450) NOT NULL PRIMARY KEY
                                 )";
                    using (var cmd = new SqlCommand(sql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Main table [{_schemaName}].[{_baseTableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                // Ensure key column
                var existingColumns = GetExistingColumns(connection, _baseTableName);
                if (!existingColumns.Contains("key", StringComparer.OrdinalIgnoreCase))
                {
                    if (_createOrUpdateTableSchema)
                    {
                        var sql = $@"ALTER TABLE [{_schemaName}].[{_baseTableName}] ADD [key] NVARCHAR(450) NOT NULL";
                        using (var cmd = new SqlCommand(sql, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        // Add primary key if missing
                        var pkCheck = $"SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('[{_schemaName}].[{_baseTableName}]') AND is_primary_key = 1";
                        using (var pkCmd = new SqlCommand(pkCheck, connection))
                        {
                            var pkExists = pkCmd.ExecuteScalar();
                            if (pkExists == null)
                            {
                                var addPk = $@"ALTER TABLE [{_schemaName}].[{_baseTableName}] ADD CONSTRAINT [PK_{_baseTableName}] PRIMARY KEY ([key])";
                                using (var addPkCmd = new SqlCommand(addPk, connection))
                                {
                                    addPkCmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"Column [key] is missing in [{_schemaName}].[{_baseTableName}] and createOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private void EnsureChildTable(SqlConnection connection, ListPropertyMetadata lp)
        {
            if (!TableExists(connection, lp.TableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var childTableSql = BuildCreateChildTableSql(lp);
                    using (var cmd = new SqlCommand(childTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Child table [{_schemaName}].[{lp.TableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                var propMap = lp.ChildScalarProperties.ToDictionary(p => p.Name, p => p);
                // For child tables we know we have key, ItemIndex plus these properties.
                // key and ItemIndex are fixed: key NVARCHAR(450), ItemIndex INT
                // Add them to propMap? key and ItemIndex are not from properties, so handle them separately.
                EnsureColumns(connection, lp.TableName, propMap, isChildTable: true, isListType: false);
            }
        }

        private void EnsureChildTableForListType(SqlConnection connection)
        {
            var tableName = _baseTableName + "_Child";

            if (!TableExists(connection, tableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var childTableSql = BuildCreateChildTableSqlForListType();
                    using (var cmd = new SqlCommand(childTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Child table [{_schemaName}].[{tableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                var propMap = _typeAnalyzer.ChildScalarProperties.ToDictionary(p => p.Name, p => p);
                EnsureColumns(connection, tableName, propMap, isChildTable: true, isListType: true);
            }
        }

        private bool TableExists(SqlConnection connection, string tableName)
        {
            var sql = $@"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table";
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@schema", _schemaName);
                cmd.Parameters.AddWithValue("@table", tableName);
                return cmd.ExecuteScalar() != null;
            }
        }

        /// <summary>
        /// Ensures all required columns exist. If not, adds them if createOrUpdateTableSchema = true.
        /// For main table: key + scalar properties
        /// For child table: key + ItemIndex + child scalar properties
        /// For list type main table: just key
        /// For list type child table: key + ItemIndex + child scalar props
        /// </summary>
        private void EnsureColumns(SqlConnection connection, string tableName, Dictionary<string, PropertyInfo> propMap, bool isChildTable, bool isListType)
        {
            var existingColumns = GetExistingColumns(connection, tableName);
            // Always ensure 'key' column
            if (!existingColumns.Contains("key", StringComparer.OrdinalIgnoreCase))
            {
                AddSpecialColumn(connection, tableName, "key", "NVARCHAR(450) NOT NULL", isPk: false); // We'll rely on PK existing or created at table creation.
            }

            if (isChildTable || (isListType && tableName.EndsWith("_Child", StringComparison.OrdinalIgnoreCase)))
            {
                // Ensure ItemIndex
                if (!existingColumns.Contains("ItemIndex", StringComparer.OrdinalIgnoreCase))
                {
                    AddSpecialColumn(connection, tableName, "ItemIndex", "INT NOT NULL", isPk: false);
                }
            }

            // Ensure property columns
            foreach (var kvp in propMap)
            {
                var colName = kvp.Key;
                var prop = kvp.Value;
                if (!existingColumns.Contains(colName, StringComparer.OrdinalIgnoreCase))
                {
                    if (_createOrUpdateTableSchema)
                    {
                        AddColumnForProperty(connection, tableName, prop);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Column [{colName}] is missing in [{_schemaName}].[{tableName}] and createOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private HashSet<string> GetExistingColumns(SqlConnection connection, string tableName)
        {
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var sql = $@"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table";
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@schema", _schemaName);
                cmd.Parameters.AddWithValue("@table", tableName);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader.GetString(0));
                    }
                }
            }
            return columns;
        }

        private void AddColumnForProperty(SqlConnection connection, string tableName, PropertyInfo prop)
        {
            var sqlType = GetSqlTypeForProperty(prop);
            var sql = $@"ALTER TABLE [{_schemaName}].[{tableName}] ADD [{prop.Name}] {sqlType} NULL";
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private void AddSpecialColumn(SqlConnection connection, string tableName, string columnName, string sqlTypeDeclaration, bool isPk)
        {
            if (!_createOrUpdateTableSchema)
            {
                throw new InvalidOperationException($"Column [{columnName}] is missing in [{_schemaName}].[{tableName}] and createOrUpdateTableSchema=false.");
            }
            var sql = $@"ALTER TABLE [{_schemaName}].[{tableName}] ADD [{columnName}] {sqlTypeDeclaration}";
            using (var cmd = new SqlCommand(sql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            if (isPk)
            {
                var pkSql = $@"ALTER TABLE [{_schemaName}].[{tableName}] ADD CONSTRAINT [PK_{tableName}] PRIMARY KEY ([{columnName}])";
                using (var pkCmd = new SqlCommand(pkSql, connection))
                {
                    pkCmd.ExecuteNonQuery();
                }
            }
        }

        private string BuildCreateMainTableSql()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE [{_schemaName}].[{_baseTableName}] (");
            sb.AppendLine("[key] NVARCHAR(450) NOT NULL PRIMARY KEY,");

            foreach (var prop in _typeAnalyzer.ScalarProperties)
            {
                var sqlType = GetSqlTypeForProperty(prop);
                sb.AppendLine($"[{prop.Name}] {sqlType} NULL,");
            }

            if (_typeAnalyzer.ScalarProperties.Length > 0)
                sb.Length -= 3; // remove last comma
            else
                sb.Length -= 2; // remove last newline if no scalar props

            sb.AppendLine(")");
            return sb.ToString();
        }

        private string BuildCreateChildTableSql(ListPropertyMetadata lp)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE [{_schemaName}].[{lp.TableName}] (");
            sb.AppendLine("[key] NVARCHAR(450) NOT NULL,");
            sb.AppendLine("[ItemIndex] INT NOT NULL,");

            foreach (var cprop in lp.ChildScalarProperties)
            {
                var sqlType = GetSqlTypeForProperty(cprop);
                sb.AppendLine($"[{cprop.Name}] {sqlType} NULL,");
            }

            sb.AppendLine($"CONSTRAINT [PK_{lp.TableName}] PRIMARY KEY ([key], [ItemIndex])");
            sb.AppendLine(")");
            return sb.ToString();
        }

        private string BuildCreateChildTableSqlForListType()
        {
            var tableName = _baseTableName + "_Child";
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE [{_schemaName}].[{tableName}] (");
            sb.AppendLine("[key] NVARCHAR(450) NOT NULL,");
            sb.AppendLine("[ItemIndex] INT NOT NULL,");

            foreach (var cprop in _typeAnalyzer.ChildScalarProperties)
            {
                var sqlType = GetSqlTypeForProperty(cprop);
                sb.AppendLine($"[{cprop.Name}] {sqlType} NULL,");
            }

            sb.AppendLine($"CONSTRAINT [PK_{tableName}] PRIMARY KEY ([key], [ItemIndex])");
            sb.AppendLine(")");
            return sb.ToString();
        }

        public string BuildUpsertMainSql()
        {
            var setClauses = string.Join(", ", _typeAnalyzer.ScalarProperties.Select(p => $"[{p.Name}] = @{p.Name}"));
            var insertColumns = string.Join(", ", _typeAnalyzer.ScalarProperties.Select(p => $"[{p.Name}]"));
            var insertValues = string.Join(", ", _typeAnalyzer.ScalarProperties.Select(p => $"@{p.Name}"));
            return $@"
                IF EXISTS (SELECT 1 FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key)
                BEGIN
                    UPDATE [{_schemaName}].[{_baseTableName}] SET {setClauses} WHERE [key] = @key;
                END
                ELSE
                BEGIN
                    INSERT INTO [{_schemaName}].[{_baseTableName}] ([key]{(insertColumns.Length > 0 ? ", " + insertColumns : "")})
                    VALUES (@key{(insertValues.Length > 0 ? ", " + insertValues : "")});
                END";
        }

        public string BuildUpsertMainSqlForListType()
        {
            // For list type: just ensure row with key
            return $@"
                IF NOT EXISTS (SELECT 1 FROM [{_schemaName}].[{_baseTableName}] WHERE [key] = @key)
                BEGIN
                    INSERT INTO [{_schemaName}].[{_baseTableName}] ([key]) VALUES (@key);
                END";
        }

        public string BuildInsertChildSql(ListPropertyMetadata lp)
        {
            var columns = new List<string> { "[key]", "[ItemIndex]" };
            columns.AddRange(lp.ChildScalarProperties.Select(p => $"[{p.Name}]"));
            var values = new List<string> { "@key", "@ItemIndex" };
            values.AddRange(lp.ChildScalarProperties.Select(p => $"@{p.Name}"));

            return $@"
                INSERT INTO [{_schemaName}].[{lp.TableName}] 
                ({string.Join(", ", columns)})
                VALUES ({string.Join(", ", values)})";
        }

        public string BuildInsertChildSqlForListType()
        {
            var tableName = _baseTableName + "_Child";
            var columns = new List<string> { "[key]", "[ItemIndex]" };
            columns.AddRange(_typeAnalyzer.ChildScalarProperties.Select(p => $"[{p.Name}]"));
            var values = new List<string> { "@key", "@ItemIndex" };
            values.AddRange(_typeAnalyzer.ChildScalarProperties.Select(p => $"@{p.Name}"));

            return $@"
                INSERT INTO [{_schemaName}].[{tableName}]
                ({string.Join(", ", columns)})
                VALUES ({string.Join(", ", values)})";
        }

        /// <summary>
        /// Maps C# property types to appropriate SQL types. You can refine this mapping as needed.
        /// </summary>
        private string GetSqlTypeForProperty(PropertyInfo prop)
        {
            var type = prop.PropertyType;
            // Unwrap nullable
            Type underlying = Nullable.GetUnderlyingType(type) ?? type;

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
                return "BIGINT";

            // We could store TimeSpan as ticks in a BIGINT column, or a string. For simplicity, store as BIGINT (ticks).
            // For TimeSpan conversion: store and retrieve ticks.

            // If string or unknown:
            if (underlying == typeof(string))
                return "NVARCHAR(MAX)";

            // Default to NVARCHAR(MAX) for unknown types
            return "NVARCHAR(MAX)";
        }
    }
}
