using Cortex.States;
using Cortex.Streams.Operators;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace Cortex.Streams.MSSqlServer
{
    /// <summary>
    /// MSSQL CDC Source Operator that optionally performs an initial load of the table,
    /// then continues reading incremental changes via CDC.
    /// Now we skip duplicates by storing a hash of the last record we emitted.
    /// </summary>
    public class SqlServerSourceOperator : ISourceOperator<SqlServerRecord>
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _tableName;
        private readonly bool _autoEnableCdc;
        private readonly bool _doInitialLoad;

        private readonly int _pollIntervalMs;
        private Thread _pollingThread;
        private bool _stopRequested;

        // Checkpoint store
        private readonly IDataStore<string, byte[]> _checkpointStore;

        // Key for the last LSN processed
        private readonly string _checkpointKey;

        // Key to track if the initial load is done
        private readonly string _initialLoadCheckpointKey;

        // Key to store the last emitted record's hash
        private readonly string _lastRecordHashKey;

        public SqlServerSourceOperator(
            string connectionString,
            string schemaName,
            string tableName,
            SqlServerSettings sqlServerSettings = null,
            IDataStore<string, byte[]> checkpointStore = null)
        {
            _connectionString = connectionString;
            _schemaName = schemaName;
            _tableName = tableName;

            sqlServerSettings ??= new SqlServerSettings();
            _autoEnableCdc = sqlServerSettings.ConfigureCDCInServer;
            _doInitialLoad = sqlServerSettings.DoInitialLoad;

            // Using Milliseconds from the original code
            _pollIntervalMs = sqlServerSettings.PullInterval.Milliseconds;

            _checkpointStore = checkpointStore
                ?? new InMemoryStateStore<string, byte[]>($"{_schemaName}.{_tableName}.STORE");

            // A unique key that identifies this table’s LSN checkpoint
            _checkpointKey = $"{_schemaName}.{_tableName}.CDC.LSN";

            // A unique key that identifies if the initial load is done
            _initialLoadCheckpointKey = $"{_schemaName}.{_tableName}.INITIAL_LOAD_DONE";

            // A unique key to store the last emitted record's hash
            _lastRecordHashKey = $"{_schemaName}.{_tableName}.CDC.LAST_HASH";
        }

        public void Start(Action<SqlServerRecord> emit)
        {
            // Optionally enable CDC if requested
            if (_autoEnableCdc && !IsCdcEnabledForTable())
            {
                EnableCdcForTable();
            }

            // 1. If doInitialLoad = true and we haven't done it yet, run initial load
            if (_doInitialLoad && _checkpointStore.Get(_initialLoadCheckpointKey) == null)
            {
                Console.WriteLine("Starting one-time initial load...");
                RunInitialLoad(emit);

                // Mark initial load as completed
                _checkpointStore.Put(_initialLoadCheckpointKey, new byte[] { 0x01 });
                Console.WriteLine("Initial load completed.");
            }
            else
            {
                Console.WriteLine("Skipping initial load (already done or disabled).");
            }

            // 2. Initialize the LSN checkpoint if we don’t already have one
            byte[] lastCommittedLsn = _checkpointStore.Get(_checkpointKey);
            if (lastCommittedLsn == null)
            {
                // By default, start from "current" so we only see future changes
                lastCommittedLsn = GetMaxLsn();
                _checkpointStore.Put(_checkpointKey, lastCommittedLsn);
            }

            // 3. Start CDC polling
            _stopRequested = false;
            _pollingThread = new Thread(() => PollCdcChanges(emit));
            _pollingThread.IsBackground = true;
            _pollingThread.Start();
        }

        public void Stop()
        {
            _stopRequested = true;
            _pollingThread?.Join();
        }

        /// <summary>
        /// Polls the CDC table periodically for new changes,
        /// using the last LSN from the checkpoint store.
        /// </summary>
        private void PollCdcChanges(Action<SqlServerRecord> emit)
        {
            string captureInstanceName = $"{_schemaName}_{_tableName}";

            while (!_stopRequested)
            {
                try
                {
                    byte[] lastCommittedLsn = _checkpointStore.Get(_checkpointKey);
                    byte[] maxLsn = GetMaxLsn();
                    if (maxLsn == null)
                    {
                        Thread.Sleep(_pollIntervalMs);
                        continue;
                    }

                    // If maxLSN <= lastCommitted, there's nothing new
                    if (CompareLsn(maxLsn, lastCommittedLsn) <= 0)
                    {
                        Thread.Sleep(_pollIntervalMs);
                        continue;
                    }

                    // Get changes
                    var changes = GetChangesSinceLastLsn(captureInstanceName, lastCommittedLsn, maxLsn);

                    // Retrieve the last record's hash we stored
                    var lastHashBytes = _checkpointStore.Get(_lastRecordHashKey);
                    string lastHash = lastHashBytes == null ? null : Encoding.UTF8.GetString(lastHashBytes);

                    // Process changes in LSN order to ensure proper checkpointing
                    foreach (var change in changes)
                    {
                        if (_stopRequested)
                            break;

                        // Compute a hash of this record
                        string currentHash = ComputeHash(change);

                        // If it's the same as the last emitted, skip
                        if (currentHash == lastHash)
                        {
                            continue;
                        }

                        // Otherwise, emit
                        emit(change);

                        // Update last-hash checkpoint
                        lastHash = currentHash;
                        _checkpointStore.Put(_lastRecordHashKey, Encoding.UTF8.GetBytes(lastHash));
                    }

                    // Update the LSN checkpoint if new changes arrived
                    if (changes.Count > 0)
                    {
                        _checkpointStore.Put(_checkpointKey, maxLsn);
                    }

                    Thread.Sleep(_pollIntervalMs);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in CDC polling: {ex}");
                    Thread.Sleep(5000);
                }
            }
        }

        /// <summary>
        /// Reads all data from the base table and emits it once.
        /// </summary>
        private void RunInitialLoad(Action<SqlServerRecord> emit)
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();
                cmd.CommandText = $"SELECT * FROM [{_schemaName}].[{_tableName}] (NOLOCK)";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var record = new SqlServerRecord
                        {
                            Operation = "InitialLoad",
                            Data = new Dictionary<string, object>(),
                            ChangeTime = DateTime.UtcNow,
                        };

                        // Populate the data dictionary with all columns
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string colName = reader.GetName(i);
                            if (colName.StartsWith("__$"))
                                continue;

                            object value = reader.GetValue(i);
                            record.Data[colName] = value == DBNull.Value ? null : value;
                        }

                        emit(record);
                    }
                }
            }
        }

        private bool IsCdcEnabledForTable()
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();

                // Check if database-level CDC is enabled
                cmd.CommandText = @"
                    SELECT is_cdc_enabled
                    FROM sys.databases
                    WHERE name = DB_NAME();";

                bool isDbCdcEnabled = Convert.ToBoolean(cmd.ExecuteScalar());
                if (!isDbCdcEnabled)
                    return false;

                // Check if table-level CDC is enabled
                cmd.CommandText = @"
                    SELECT COUNT(*)
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = @schemaName
                      AND t.name = @tableName
                      AND t.is_tracked_by_cdc = 1;";

                cmd.Parameters.AddWithValue("@schemaName", _schemaName);
                cmd.Parameters.AddWithValue("@tableName", _tableName);

                int count = (int)cmd.ExecuteScalar();
                return (count > 0);
            }
        }

        private void EnableCdcForTable()
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();

                // 1. Enable CDC at the database level if not already
                cmd.CommandText = @"
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.databases
                        WHERE name = DB_NAME() AND is_cdc_enabled = 1
                    )
                    BEGIN
                        EXEC sys.sp_cdc_enable_db;
                    END
                ";
                cmd.ExecuteNonQuery();

                // 2. Enable CDC on the table
                cmd.CommandText = $@"
                    EXEC sys.sp_cdc_enable_table
                        @source_schema = '{_schemaName}',
                        @source_name   = '{_tableName}',
                        @capture_instance = '{_schemaName}_{_tableName}',
                        @role_name = NULL,
                        @supports_net_changes = 0;
                ";
                cmd.ExecuteNonQuery();

                Console.WriteLine($"CDC enabled for table [{_schemaName}].[{_tableName}].");
            }
        }

        private byte[] GetMaxLsn()
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();
                cmd.CommandText = "SELECT sys.fn_cdc_get_max_lsn();";
                object result = cmd.ExecuteScalar();
                if (result == DBNull.Value) return null;
                return (byte[])result;
            }
        }

        private byte[] GetMinLsn(string captureInstanceName)
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();
                cmd.CommandText =
                    $"SELECT sys.fn_cdc_get_min_lsn('{captureInstanceName}');";
                object result = cmd.ExecuteScalar();
                if (result == DBNull.Value) return null;
                return (byte[])result;
            }
        }

        private List<SqlServerRecord> GetChangesSinceLastLsn(
            string captureInstanceName,
            byte[] fromLsn,
            byte[] toLsn)
        {
            var changes = new List<SqlServerRecord>();
            string functionName = $"cdc.fn_cdc_get_all_changes_{captureInstanceName}";

            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();
                // Use 'all update old' to get both old & new update rows
                cmd.CommandText = $@"
            SELECT *
            FROM {functionName}(@from_lsn, @to_lsn, 'all update old')
        ";

                cmd.Parameters.Add(new SqlParameter("@from_lsn", SqlDbType.VarBinary, 10) { Value = fromLsn });
                cmd.Parameters.Add(new SqlParameter("@to_lsn", SqlDbType.VarBinary, 10) { Value = toLsn });

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var record = new SqlServerRecord
                        {
                            Operation = GetOperationName(Convert.ToInt32(reader["__$operation"])),
                            Data = new Dictionary<string, object>(),
                            ChangeTime = DateTime.UtcNow,
                        };

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string colName = reader.GetName(i);
                            if (colName.StartsWith("__$")) continue;

                            object value = reader.GetValue(i);
                            record.Data[colName] = (value == DBNull.Value) ? null : value;
                        }

                        changes.Add(record);
                    }
                }
            }

            // Filter out rows that are the "old" side of an update
            // (operation code 3 = "Update (old)")
            changes = changes
                .Where(c => c.Operation != "Update (old)")
                .ToList();

            return changes;
        }



        private string GetOperationName(int operationCode)
        {
            switch (operationCode)
            {
                case 1: return "Delete (old)";
                case 2: return "Insert";
                case 3: return "Update (old)";
                case 4: return "Update (new)";
                case 5: return "Delete (new)";
                default: return $"Unknown ({operationCode})";
            }
        }

        /// <summary>
        /// Compare two varbinary(10) LSNs:
        ///  -1 if lsnA < lsnB
        ///   0 if lsnA == lsnB
        ///   1 if lsnA > lsnB
        /// </summary>
        private int CompareLsn(byte[] lsnA, byte[] lsnB)
        {
            if (lsnA == null && lsnB == null) return 0;
            if (lsnA == null) return -1;
            if (lsnB == null) return 1;

            for (int i = 0; i < 10; i++)
            {
                if (lsnA[i] < lsnB[i]) return -1;
                if (lsnA[i] > lsnB[i]) return 1;
            }
            return 0;
        }

        /// <summary>
        /// Computes an MD5 hash from the SqlServerRecord's Data dictionary.
        /// You could also use JSON serialization, SHA256, etc.
        /// </summary>
        private string ComputeHash(SqlServerRecord record)
        {
            // Build a stable string from the record's Data
            // Sort by key so that the order is deterministic
            var sb = new StringBuilder();
            foreach (var kv in record.Data.OrderBy(x => x.Key))
            {
                // "Key=Value;"
                sb.Append(kv.Key).Append('=').Append(kv.Value ?? "null").Append(';');
            }

            // Get MD5 hash of that string
            using (var md5 = MD5.Create())
            {
                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                var hashBytes = md5.ComputeHash(bytes);
                return Convert.ToBase64String(hashBytes);
            }
        }
    }
}
