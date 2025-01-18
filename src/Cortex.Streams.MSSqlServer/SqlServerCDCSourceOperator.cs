using Cortex.States;
using Cortex.Streams.Operators;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Cortex.Streams.MSSqlServer
{
    /// <summary>
    /// MSSQL CDC (Change Data Capture) Source Operator that optionally performs an initial load of the table,
    /// then continues reading incremental changes via CDC. 
    /// Duplicates are skipped by storing and comparing a hash of the last record emitted.
    /// </summary>
    public class SqlServerCDCSourceOperator : ISourceOperator<SqlServerRecord>
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

        // Optional logger (may be null)
        private readonly ILogger<SqlServerCDCSourceOperator> _logger;

        /// <summary>
        /// Creates a new instance of <see cref="SqlServerCDCSourceOperator"/> which can optionally 
        /// perform an initial load of the table, then read incremental changes via CDC.
        /// Duplicates are skipped by storing a hash of the last record emitted.
        /// </summary>
        /// <param name="connectionString">The connection string to the SQL Server instance.</param>
        /// <param name="schemaName">The schema name containing the table.</param>
        /// <param name="tableName">The table name from which to read changes.</param>
        /// <param name="sqlServerSettings">Optional settings to configure CDC and polling intervals.</param>
        /// <param name="checkpointStore">Optional data store for saving checkpoints (LSN, last-hash, etc.).</param>
        /// <param name="logger">
        /// An optional <see cref="ILogger{SqlServerCDCSourceOperator}"/>. If null, messages will be written to the console.
        /// </param>
        public SqlServerCDCSourceOperator(
            string connectionString,
            string schemaName,
            string tableName,
            SqlServerSettings sqlServerSettings = null,
            IDataStore<string, byte[]> checkpointStore = null,
            ILogger<SqlServerCDCSourceOperator> logger = null) // <-- Optional ILogger
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

            // Store the logger (can be null)
            _logger = logger;
        }

        /// <summary>
        /// Starts the CDC operator by optionally enabling CDC on the table (if requested),
        /// performing a one-time initial load (if requested), and starting the background thread
        /// that polls for new changes.
        /// </summary>
        /// <param name="emit">Action to call for each new record.</param>
        public void Start(Action<SqlServerRecord> emit)
        {
            // Optionally enable CDC if requested
            if (_autoEnableCdc && !IsCdcEnabledForTable())
            {
                LogInformation("Enabling CDC for table...");
                EnableCdcForTable();

                // Sleep for 10s to allow MS SQL Server to enable CDC on the table
                Thread.Sleep(10000);
            }

            // 1. If doInitialLoad = true and we haven't done it yet, run initial load
            if (_doInitialLoad && _checkpointStore.Get(_initialLoadCheckpointKey) == null)
            {
                LogInformation("Starting one-time initial load...");
                RunInitialLoad(emit);

                // Mark initial load as completed
                _checkpointStore.Put(_initialLoadCheckpointKey, new byte[] { 0x01 });
                LogInformation("Initial load completed.");
            }
            else
            {
                LogInformation("Skipping initial load (already done or disabled).");
            }

            // 2. Initialize the LSN checkpoint if we don’t already have one
            byte[] lastCommittedLsn = _checkpointStore.Get(_checkpointKey);
            if (lastCommittedLsn == null)
            {
                // By default, start from "current" so we only see future changes
                lastCommittedLsn = GetMaxLsn();
                if (lastCommittedLsn != null)
                {
                    _checkpointStore.Put(_checkpointKey, lastCommittedLsn);
                    LogInformation("Initialized LSN checkpoint to the current max LSN.");
                }
            }

            // 3. Start CDC polling
            _stopRequested = false;
            _pollingThread = new Thread(() => PollCdcChanges(emit))
            {
                IsBackground = true
            };
            _pollingThread.Start();
            LogInformation("CDC polling thread started.");
        }

        /// <summary>
        /// Stops the CDC operator by signaling the background thread to stop and waiting for it to finish.
        /// </summary>
        public void Stop()
        {
            LogInformation("Stop requested. Waiting for polling thread to complete...");
            _stopRequested = true;
            _pollingThread?.Join();
            LogInformation("Polling thread stopped.");
        }

        /// <summary>
        /// Polls the CDC function table periodically for new changes, using the last LSN from
        /// the checkpoint store, and emits each new record found.
        /// </summary>
        /// <param name="emit">Action to call for each new record.</param>
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
                            continue;

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
                        LogInformation($"Updated LSN checkpoint to: {BitConverter.ToString(maxLsn)}");
                    }

                    Thread.Sleep(_pollIntervalMs);
                }
                catch (Exception ex)
                {
                    LogError("Error in CDC polling.", ex);
                    // Backoff a bit
                    Thread.Sleep(5000);
                }
            }
        }

        /// <summary>
        /// Reads all data from the base table once and emits it, primarily used for the one-time initial load.
        /// </summary>
        /// <param name="emit">Action to call for each record.</param>
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

        /// <summary>
        /// Checks whether CDC is enabled for the current database and table.
        /// </summary>
        /// <returns>True if CDC is enabled for this table, otherwise false.</returns>
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

        /// <summary>
        /// Enables CDC at the database level (if needed) and on the specific table.
        /// </summary>
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

                LogInformation($"CDC enabled for table [{_schemaName}].[{_tableName}].");
            }
        }

        /// <summary>
        /// Retrieves the current maximum LSN from the server.
        /// </summary>
        /// <returns>The varbinary(10) max LSN, or null if none.</returns>
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

        /// <summary>
        /// Retrieves the minimum LSN for the given capture instance.
        /// </summary>
        /// <param name="captureInstanceName">The capture instance name (schema_table).</param>
        /// <returns>The varbinary(10) min LSN, or null if none.</returns>
        private byte[] GetMinLsn(string captureInstanceName)
        {
            using (var conn = new SqlConnection(_connectionString))
            using (var cmd = conn.CreateCommand())
            {
                conn.Open();
                cmd.CommandText = $"SELECT sys.fn_cdc_get_min_lsn('{captureInstanceName}');";
                object result = cmd.ExecuteScalar();
                if (result == DBNull.Value) return null;
                return (byte[])result;
            }
        }

        /// <summary>
        /// Queries the CDC function table for changes between the specified LSN range.
        /// Filters out "old" updates (operation 3).
        /// </summary>
        /// <param name="captureInstanceName">The capture instance name (schema_table).</param>
        /// <param name="fromLsn">Starting LSN (inclusive).</param>
        /// <param name="toLsn">Ending LSN (inclusive).</param>
        /// <returns>A list of new or updated records.</returns>
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

            // Filter out rows that are the "old" side of an update (operation code 3 = "Update (old)")
            changes = changes
                .Where(c => c.Operation != "Update (old)")
                .ToList();

            return changes;
        }

        /// <summary>
        /// Returns a human-readable operation name from a CDC operation code.
        /// </summary>
        /// <param name="operationCode">The integer CDC operation code.</param>
        /// <returns>A descriptive string for the operation.</returns>
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
        /// -1 if lsnA &lt; lsnB,
        ///  0 if lsnA == lsnB,
        ///  1 if lsnA &gt; lsnB.
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
        /// Computes an MD5 hash from the SqlServerRecord's Data dictionary 
        /// to detect duplicates. The data is sorted by key for deterministic ordering.
        /// </summary>
        /// <param name="record">The <see cref="SqlServerRecord"/> to hash.</param>
        /// <returns>A Base64-encoded MD5 hash string.</returns>
        private string ComputeHash(SqlServerRecord record)
        {
            var sb = new StringBuilder();
            foreach (var kv in record.Data.OrderBy(x => x.Key))
            {
                // "Key=Value;"
                sb.Append(kv.Key).Append('=').Append(kv.Value ?? "null").Append(';');
            }

            using (var md5 = MD5.Create())
            {
                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                var hashBytes = md5.ComputeHash(bytes);
                return Convert.ToBase64String(hashBytes);
            }
        }

        #region Logging Helpers

        private void LogInformation(string message)
        {
            if (_logger != null)
            {
                _logger.LogInformation(message);
            }
            else
            {
                Console.WriteLine(message);
            }
        }

        private void LogError(string message, Exception ex)
        {
            if (_logger != null)
            {
                _logger.LogError(ex, message);
            }
            else
            {
                Console.WriteLine($"ERROR: {message}\n{ex}");
            }
        }

        #endregion
    }
}
