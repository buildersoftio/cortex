using ClickHouse.Client.ADO.Parameters;
using ClickHouse.Client.ADO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.States.ClickHouse
{
    internal class ClickHouseSchemaManager
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly ClickHouseConfiguration _config;
        private readonly ClickHouseTypeAnalyzer _typeAnalyzer;

        public ClickHouseSchemaManager(
            string connectionString,
            string tableName,
            ClickHouseConfiguration config,
            ClickHouseTypeAnalyzer typeAnalyzer)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _config = config;
            _typeAnalyzer = typeAnalyzer;
        }

        public void EnsureSchemaAndTable()
        {
            // 1. Check if table exists
            if (!TableExists())
            {
                if (_config.CreateOrUpdateTableSchema)
                {
                    CreateTable();
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Table {_tableName} does not exist and CreateOrUpdateTableSchema=false.");
                }
            }
            else
            {
                // 2. Potentially check columns if table already exists.
                var existingColumns = GetExistingColumns();
                var neededColumns = BuildNeededColumns();
                foreach (var needed in neededColumns)
                {
                    if (!existingColumns.Contains(needed.Key, StringComparer.OrdinalIgnoreCase))
                    {
                        if (_config.CreateOrUpdateTableSchema)
                            AddColumn(needed);
                        else
                            throw new InvalidOperationException(
                                $"Column {needed.Key} is missing in table {_tableName} but CreateOrUpdateTableSchema=false.");
                    }
                }
            }
        }

        private bool TableExists()
        {
            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            var sql = @"SELECT count(*) 
                        FROM system.tables 
                        WHERE database = currentDatabase() 
                          AND name = @table";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = _tableName });
            var result = cmd.ExecuteScalar();
            return Convert.ToInt64(result) > 0;
        }

        private HashSet<string> GetExistingColumns()
        {
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            var sql = @"SELECT name 
                        FROM system.columns 
                        WHERE database = currentDatabase() 
                          AND table = @table";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = _tableName });
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                columns.Add(reader.GetString(0));
            }
            return columns;
        }

        private Dictionary<string, string> BuildNeededColumns()
        {
            // Always need a key column
            var needed = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "key", "String" }
            };

            // Also for a ReplacingMergeTree we typically need a version or an is_deleted or something.
            // For simplicity, let's store a 'timestamp' column:
            needed["timestamp"] = "DateTime64(3)"; // millisecond resolution

            // Map each scalar property
            foreach (var prop in _typeAnalyzer.ScalarProperties)
            {
                var chType = GetClickhouseTypeForProperty(prop);
                needed[prop.Name] = chType;
            }

            // Map each list property to an Array(...) type
            foreach (var listProp in _typeAnalyzer.ListProperties)
            {
                var itemType = listProp.PropertyType.GetGenericArguments()[0];
                var elementChType = GetClickhouseScalarType(itemType);
                needed[listProp.Name] = $"Array({elementChType})";
            }

            return needed;
        }

        private void CreateTable()
        {
            var needed = BuildNeededColumns();

            // Build CREATE TABLE statement
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {_tableName} (");
            // Add each column definition
            foreach (var kv in needed)
            {
                sb.AppendLine($"    `{kv.Key}` {kv.Value},");
            }
            // Remove last comma
            sb.Length -= 3;
            sb.AppendLine(")");

            sb.AppendLine("ENGINE = " + BuildEngineClause());

            // Example: ReplacingMergeTree(timestamp) ORDER BY key
            // or: MergeTree ORDER BY key
            // or: ReplacingMergeTree ORDER BY (key) PARTITION BY ...
            // etc.

            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        private void AddColumn(KeyValuePair<string, string> column)
        {
            var sql = $"ALTER TABLE {_tableName} ADD COLUMN `{column.Key}` {column.Value}";
            using var conn = new ClickHouseConnection(_connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private string BuildEngineClause()
        {
            // e.g. "MergeTree ORDER BY key"
            // or "ReplacingMergeTree(timestamp) ORDER BY key"
            switch (_config.TableEngine)
            {
                case ClickHouseTableEngine.ReplacingMergeTree:
                    // For ReplacingMergeTree we usually specify a version or a column like "timestamp"
                    return $"ReplacingMergeTree(timestamp) {_config.EngineModifiers}";
                default:
                    // Default is MergeTree
                    return $"MergeTree {_config.EngineModifiers}";
            }
        }

        private string GetClickhouseTypeForProperty(PropertyInfo prop)
        {
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            return GetClickhouseScalarType(type);
        }

        private string GetClickhouseScalarType(Type type)
        {
            // Basic mapping
            if (type == typeof(int) || type == typeof(short) || type == typeof(byte))
                return "Int32";
            if (type == typeof(long))
                return "Int64";
            if (type == typeof(bool))
                return "UInt8"; // or use Int8, but bool->UInt8 is common
            if (type == typeof(DateTime))
                return "DateTime64(3)";
            if (type == typeof(decimal))
                return "Decimal(18,4)"; // adjust as needed
            if (type == typeof(double))
                return "Float64";
            if (type == typeof(float))
                return "Float32";
            if (type == typeof(Guid))
                return "String"; // ClickHouse doesn't have a native GUID type in older versions
            if (type == typeof(TimeSpan))
                return "Int64"; // store ticks

            // default to String if unknown
            if (type == typeof(string))
                return "String";

            return "String";
        }
    }
}
