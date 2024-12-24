using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.PostgreSQL
{
    internal class PostgresSchemaManager
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _baseTableName;
        private readonly PostgresTypeAnalyzer _typeAnalyzer;
        private readonly bool _createOrUpdateTableSchema;

        public PostgresSchemaManager(string connectionString, string schemaName, string baseTableName, PostgresTypeAnalyzer typeAnalyzer, bool createOrUpdateTableSchema)
        {
            _connectionString = connectionString;
            _schemaName = schemaName;
            _baseTableName = baseTableName;
            _typeAnalyzer = typeAnalyzer;
            _createOrUpdateTableSchema = createOrUpdateTableSchema;
        }

        public void EnsureSchemaAndTables()
        {
            using (var connection = new NpgsqlConnection(_connectionString))
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

        private void EnsureSchema(NpgsqlConnection connection)
        {
            // CREATE SCHEMA IF NOT EXISTS
            var createSchemaSql = $@"CREATE SCHEMA IF NOT EXISTS ""{_schemaName}""";
            using (var cmd = new NpgsqlCommand(createSchemaSql, connection))
            {
                if (_createOrUpdateTableSchema)
                {
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    // Check existence
                    var checkSchemaSql = @"SELECT 1 FROM pg_namespace WHERE nspname = @schema";
                    using (var checkCmd = new NpgsqlCommand(checkSchemaSql, connection))
                    {
                        checkCmd.Parameters.AddWithValue("schema", _schemaName);
                        var exists = checkCmd.ExecuteScalar();
                        if (exists == null)
                            throw new InvalidOperationException($"Schema {_schemaName} does not exist and createOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private void EnsureMainTable(NpgsqlConnection connection)
        {
            if (!TableExists(connection, _baseTableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var mainTableSql = BuildCreateMainTableSql();
                    using (var cmd = new NpgsqlCommand(mainTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Main table [{_schemaName}.{_baseTableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                // Check columns
                var propMap = _typeAnalyzer.ScalarProperties.ToDictionary(p => p.Name, p => p);
                EnsureColumns(connection, _baseTableName, propMap, isChildTable: false, isListType: false);
            }
        }

        private void EnsureMainTableForListType(NpgsqlConnection connection)
        {
            if (!TableExists(connection, _baseTableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var sql = $@"CREATE TABLE ""{_schemaName}"".""{_baseTableName}"" (
                                 ""key"" TEXT NOT NULL PRIMARY KEY
                                 )";
                    using (var cmd = new NpgsqlCommand(sql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Main table [{_schemaName}.{_baseTableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                // Ensure key column exists
                var existingColumns = GetExistingColumns(connection, _baseTableName);
                if (!existingColumns.Contains("key"))
                {
                    if (_createOrUpdateTableSchema)
                    {
                        var alter = $@"ALTER TABLE ""{_schemaName}"".""{_baseTableName}"" ADD COLUMN ""key"" TEXT NOT NULL";
                        using (var cmd = new NpgsqlCommand(alter, connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        // Ensure primary key
                        var pkSql = $@"ALTER TABLE ""{_schemaName}"".""{_baseTableName}"" ADD PRIMARY KEY(""key"")";
                        using (var pkCmd = new NpgsqlCommand(pkSql, connection))
                        {
                            pkCmd.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"Column [key] is missing in [{_schemaName}.{_baseTableName}] and createOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private void EnsureChildTable(NpgsqlConnection connection, ListPropertyMetadata lp)
        {
            if (!TableExists(connection, lp.TableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var childTableSql = BuildCreateChildTableSql(lp);
                    using (var cmd = new NpgsqlCommand(childTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Child table [{_schemaName}.{lp.TableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                var propMap = lp.ChildScalarProperties.ToDictionary(p => p.Name, p => p);
                EnsureColumns(connection, lp.TableName, propMap, isChildTable: true, isListType: false);
            }
        }

        private void EnsureChildTableForListType(NpgsqlConnection connection)
        {
            var tableName = _baseTableName + "_Child";

            if (!TableExists(connection, tableName))
            {
                if (_createOrUpdateTableSchema)
                {
                    var childTableSql = BuildCreateChildTableSqlForListType();
                    using (var cmd = new NpgsqlCommand(childTableSql, connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Child table [{_schemaName}.{tableName}] does not exist and createOrUpdateTableSchema=false.");
                }
            }
            else
            {
                var propMap = _typeAnalyzer.ChildScalarProperties.ToDictionary(p => p.Name, p => p);
                EnsureColumns(connection, tableName, propMap, isChildTable: true, isListType: true);
            }
        }

        private bool TableExists(NpgsqlConnection connection, string tableName)
        {
            var sql = @"SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = @schema AND table_name = @table";
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("schema", _schemaName);
                cmd.Parameters.AddWithValue("table", tableName);
                return cmd.ExecuteScalar() != null;
            }
        }

        private void EnsureColumns(NpgsqlConnection connection, string tableName, Dictionary<string, PropertyInfo> propMap, bool isChildTable, bool isListType)
        {
            var existingColumns = GetExistingColumns(connection, tableName);

            // Always ensure "key"
            if (!existingColumns.Contains("key"))
            {
                AddSpecialColumn(connection, tableName, "key", "TEXT NOT NULL", isPk: false);
            }

            if (isChildTable || (isListType && tableName.EndsWith("_Child", StringComparison.OrdinalIgnoreCase)))
            {
                // Ensure ItemIndex
                if (!existingColumns.Contains("itemindex"))
                {
                    AddSpecialColumn(connection, tableName, "ItemIndex", "INTEGER NOT NULL", isPk: false);
                }
            }

            // Ensure property columns
            foreach (var kvp in propMap)
            {
                var colName = kvp.Key;
                var prop = kvp.Value;
                if (!existingColumns.Contains(colName.ToLower()))
                {
                    if (_createOrUpdateTableSchema)
                    {
                        AddColumnForProperty(connection, tableName, prop);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Column [{colName}] is missing in [{_schemaName}.{tableName}] and createOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private HashSet<string> GetExistingColumns(NpgsqlConnection connection, string tableName)
        {
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var sql = @"SELECT column_name FROM information_schema.columns 
                        WHERE table_schema = @schema AND table_name = @table";
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("schema", _schemaName);
                cmd.Parameters.AddWithValue("table", tableName);
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

        private void AddColumnForProperty(NpgsqlConnection connection, string tableName, PropertyInfo prop)
        {
            var sqlType = GetPostgresTypeForProperty(prop);
            var sql = $@"ALTER TABLE ""{_schemaName}"".""{tableName}"" ADD COLUMN ""{prop.Name}"" {sqlType} NULL";
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private void AddSpecialColumn(NpgsqlConnection connection, string tableName, string columnName, string sqlTypeDeclaration, bool isPk)
        {
            if (!_createOrUpdateTableSchema)
            {
                throw new InvalidOperationException($"Column [{columnName}] is missing in [{_schemaName}.{tableName}] and createOrUpdateTableSchema=false.");
            }
            var sql = $@"ALTER TABLE ""{_schemaName}"".""{tableName}"" ADD COLUMN ""{columnName}"" {sqlTypeDeclaration}";
            using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            if (isPk)
            {
                var pkSql = $@"ALTER TABLE ""{_schemaName}"".""{tableName}"" ADD PRIMARY KEY(""{columnName}"")";
                using (var pkCmd = new NpgsqlCommand(pkSql, connection))
                {
                    pkCmd.ExecuteNonQuery();
                }
            }
        }

        private string BuildCreateMainTableSql()
        {
            var sb = new StringBuilder();
            sb.AppendLine($@"CREATE TABLE ""{_schemaName}"".""{_baseTableName}"" (");
            sb.AppendLine(@"""key"" TEXT NOT NULL PRIMARY KEY,");

            foreach (var prop in _typeAnalyzer.ScalarProperties)
            {
                var sqlType = GetPostgresTypeForProperty(prop);
                sb.AppendLine($@"""{prop.Name}"" {sqlType} NULL,");
            }

            if (_typeAnalyzer.ScalarProperties.Length > 0)
                sb.Length -= 3; // remove last comma
            else
                sb.Length -= 2; // if no scalar props

            sb.AppendLine(")");
            return sb.ToString();
        }

        private string BuildCreateChildTableSql(ListPropertyMetadata lp)
        {
            var sb = new StringBuilder();
            sb.AppendLine($@"CREATE TABLE ""{_schemaName}"".""{lp.TableName}"" (");
            sb.AppendLine(@"""key"" TEXT NOT NULL,");
            sb.AppendLine(@"""ItemIndex"" INTEGER NOT NULL,");

            foreach (var cprop in lp.ChildScalarProperties)
            {
                var sqlType = GetPostgresTypeForProperty(cprop);
                sb.AppendLine($@"""{cprop.Name}"" {sqlType} NULL,");
            }

            sb.AppendLine($@"CONSTRAINT ""PK_{lp.TableName}"" PRIMARY KEY (""key"", ""ItemIndex"")");
            sb.AppendLine(")");
            return sb.ToString();
        }

        private string BuildCreateChildTableSqlForListType()
        {
            var tableName = _baseTableName + "_Child";
            var sb = new StringBuilder();
            sb.AppendLine($@"CREATE TABLE ""{_schemaName}"".""{tableName}"" (");
            sb.AppendLine(@"""key"" TEXT NOT NULL,");
            sb.AppendLine(@"""ItemIndex"" INTEGER NOT NULL,");

            foreach (var cprop in _typeAnalyzer.ChildScalarProperties)
            {
                var sqlType = GetPostgresTypeForProperty(cprop);
                sb.AppendLine($@"""{cprop.Name}"" {sqlType} NULL,");
            }

            sb.AppendLine($@"CONSTRAINT ""PK_{tableName}"" PRIMARY KEY (""key"", ""ItemIndex"")");
            sb.AppendLine(")");
            return sb.ToString();
        }

        public string BuildUpsertMainSql()
        {
            // Postgres UPSERT: use INSERT ... ON CONFLICT
            var columns = _typeAnalyzer.ScalarProperties.Select(p => p.Name).ToArray();
            var insertCols = string.Join(", ", columns.Select(c => $@"""{c}"""));
            var insertVals = string.Join(", ", columns.Select(c => "@" + c));
            var updateSet = string.Join(", ", columns.Select(c => $@"""{c}"" = EXCLUDED.""{c}"""));

            // If no scalar columns, we just insert key if not exists
            if (columns.Length == 0)
            {
                return $@"
                INSERT INTO ""{_schemaName}"".""{_baseTableName}"" (""key"") 
                VALUES (@key)
                ON CONFLICT (""key"") DO NOTHING;";
            }

            return $@"
                INSERT INTO ""{_schemaName}"".""{_baseTableName}"" (""key"", {insertCols})
                VALUES (@key, {insertVals})
                ON CONFLICT (""key"") DO UPDATE SET {updateSet};";
        }

        public string BuildUpsertMainSqlForListType()
        {
            return $@"
                INSERT INTO ""{_schemaName}"".""{_baseTableName}"" (""key"")
                VALUES (@key)
                ON CONFLICT (""key"") DO NOTHING;";
        }

        public string BuildInsertChildSql(ListPropertyMetadata lp)
        {
            var columns = new List<string> { "\"key\"", "\"ItemIndex\"" };
            columns.AddRange(lp.ChildScalarProperties.Select(p => $@"""{p.Name}"""));
            var values = new List<string> { "@key", "@ItemIndex" };
            values.AddRange(lp.ChildScalarProperties.Select(p => "@" + p.Name));

            return $@"
                INSERT INTO ""{_schemaName}"".""{lp.TableName}"" 
                ({string.Join(", ", columns)})
                VALUES ({string.Join(", ", values)})";
        }

        public string BuildInsertChildSqlForListType()
        {
            var tableName = _baseTableName + "_Child";
            var columns = new List<string> { "\"key\"", "\"ItemIndex\"" };
            columns.AddRange(_typeAnalyzer.ChildScalarProperties.Select(p => $@"""{p.Name}"""));
            var values = new List<string> { "@key", "@ItemIndex" };
            values.AddRange(_typeAnalyzer.ChildScalarProperties.Select(p => "@" + p.Name));

            return $@"
                INSERT INTO ""{_schemaName}"".""{tableName}""
                ({string.Join(", ", columns)})
                VALUES ({string.Join(", ", values)})";
        }

        private string GetPostgresTypeForProperty(PropertyInfo prop)
        {
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
                return "INTEGER";
            if (type == typeof(long))
                return "BIGINT";
            if (type == typeof(bool))
                return "BOOLEAN";
            if (type == typeof(DateTime))
                return "TIMESTAMP";
            if (type == typeof(decimal))
                return "NUMERIC(18,2)";
            if (type == typeof(double))
                return "DOUBLE PRECISION";
            if (type == typeof(float))
                return "REAL";
            if (type == typeof(Guid))
                return "UUID";
            if (type == typeof(TimeSpan))
                return "INTERVAL";
            if (type == typeof(string))
                return "TEXT";

            // fallback
            return "TEXT";
        }
    }
}
