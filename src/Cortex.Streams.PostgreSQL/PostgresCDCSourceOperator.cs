using Cortex.States;
using Cortex.Streams.Operators;
using Npgsql;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Streams.PostgreSQL
{
    /// <summary>
    /// PostgreSQL CDC Source Operator that:
    /// 1. Optionally configures logical replication for a table (via publication + slot).
    /// 2. Performs an initial load of the table if requested.
    /// 3. Polls changes from a replication slot using wal2json.
    /// 4. Emits new records, skipping duplicates with a hashed checkpoint.
    /// 5. Uses robust error handling and exponential back-off.
    /// </summary>
    public class PostgresSourceOperator : ISourceOperator<PostgresRecord>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _schemaName;
        private readonly string _tableName;
        private readonly bool _autoEnableCdc;
        private readonly bool _doInitialLoad;
        private readonly ReplicaIdentityMode _replicaIdentity;

        private readonly int _pollIntervalMs;
        private readonly int _maxBackOffSeconds;

        private readonly IDataStore<string, byte[]> _checkpointStore;

        // Keys for checkpoint store
        private readonly string _checkpointKey;
        private readonly string _initialLoadCheckpointKey;
        private readonly string _lastRecordHashKey;

        // Logical replication details
        private readonly string _slotName;
        private readonly string _publicationName;

        // Thread & cancellation
        private Thread _pollingThread;
        private bool _stopRequested;
        private bool _disposed;

        // Optional logger
        private readonly ILogger<PostgresSourceOperator> _logger;

        public PostgresSourceOperator(
            string connectionString,
            string schemaName,
            string tableName,
            string slotName = "my_slot",
            string publicationName = "my_publication",
            PostgresSettings postgresSettings = null,
            IDataStore<string, byte[]> checkpointStore = null,
            ILogger<PostgresSourceOperator> logger = null) // <-- Optional ILogger
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
            if (string.IsNullOrWhiteSpace(schemaName))
                throw new ArgumentException("Schema name cannot be null or empty.", nameof(schemaName));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));

            postgresSettings ??= new PostgresSettings();

            _checkpointStore = checkpointStore
                ?? new InMemoryStateStore<string, byte[]>($"{schemaName}.{tableName}.STORE");

            _connectionString = connectionString;
            _schemaName = schemaName;
            _tableName = tableName;

            _autoEnableCdc = postgresSettings.ConfigureCDCInServer;
            _doInitialLoad = postgresSettings.DoInitialLoad;
            _replicaIdentity = postgresSettings.ReplicaIdentity;
            _pollIntervalMs = (int)postgresSettings.PullInterval.TotalMilliseconds;
            _maxBackOffSeconds = postgresSettings.MaxBackOffSeconds;

            // Checkpoint keys
            _checkpointKey = $"{_schemaName}.{_tableName}.CDC.LSN";
            _initialLoadCheckpointKey = $"{_schemaName}.{_tableName}.INITIAL_LOAD_DONE";
            _lastRecordHashKey = $"{_schemaName}.{_tableName}.CDC.LAST_HASH";

            _slotName = slotName;
            _publicationName = publicationName;

            // Store logger (can be null)
            _logger = logger;
        }

        /// <summary>
        /// Start the operator. This sets up CDC (if requested) and launches the polling thread.
        /// </summary>
        public void Start(Action<PostgresRecord> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));

            LogInformation($"Starting PostgreSQL CDC operator for {_schemaName}.{_tableName}...");

            // 1. Optionally configure logical replication (publication + slot).
            if (_autoEnableCdc)
            {
                EnsureLogicalReplicationSetup();
            }

            // 2. Perform initial load if needed.
            if (_doInitialLoad && _checkpointStore.Get(_initialLoadCheckpointKey) == null)
            {
                LogInformation($"Performing initial load for {_schemaName}.{_tableName}...");
                RunInitialLoad(emit);
                _checkpointStore.Put(_initialLoadCheckpointKey, new byte[] { 0x01 });
                LogInformation($"Initial load completed for {_schemaName}.{_tableName}");
            }
            else
            {
                LogInformation($"Skipping initial load for {_schemaName}.{_tableName} (already done or disabled).");
            }

            // 3. If we have no saved LSN, we can either start from the slot's current position or from scratch.
            var lastLsnBytes = _checkpointStore.Get(_checkpointKey);
            if (lastLsnBytes == null)
            {
                LogInformation($"No existing LSN checkpoint found for {_schemaName}.{_tableName}. Will start from slot's current position.");
            }
            else
            {
                var lastLsn = Encoding.UTF8.GetString(lastLsnBytes);
                LogInformation($"Found existing LSN checkpoint = {lastLsn} for {_schemaName}.{_tableName}.");
            }

            // 4. Spin up the background polling thread.
            _stopRequested = false;
            _pollingThread = new Thread(() => PollCdcChanges(emit))
            {
                IsBackground = true,
                Name = $"PostgresCdcPolling_{_schemaName}_{_tableName}"
            };
            _pollingThread.Start();
        }

        /// <summary>
        /// Stop the operator gracefully.
        /// </summary>
        public void Stop()
        {
            LogInformation($"Stop requested for PostgreSQL CDC operator {_schemaName}.{_tableName}.");
            _stopRequested = true;
            _pollingThread?.Join();
            LogInformation($"PostgreSQL CDC operator stopped for {_schemaName}.{_tableName}.");
        }

        /// <summary>
        /// Implements the main CDC polling logic with exponential back-off on errors.
        /// </summary>
        private void PollCdcChanges(Action<PostgresRecord> emit)
        {
            int backOffSeconds = 1;

            while (!_stopRequested)
            {
                try
                {
                    var lastLsnBytes = _checkpointStore.Get(_checkpointKey);
                    string lastLsn = lastLsnBytes == null ? null : Encoding.UTF8.GetString(lastLsnBytes);

                    // Retrieve new changes from the slot
                    var (newChanges, latestLsn) = GetChangesSinceLastLsn(lastLsn);

                    // Retrieve the last record's hash we stored (for dedup)
                    var lastHashBytes = _checkpointStore.Get(_lastRecordHashKey);
                    string lastHash = lastHashBytes == null ? null : Encoding.UTF8.GetString(lastHashBytes);

                    // Emit each new record
                    foreach (var change in newChanges)
                    {
                        if (_stopRequested) break;

                        // Compute a hash of this record to detect duplicates
                        var currentHash = ComputeHash(change);
                        if (currentHash == lastHash)
                        {
                            LogInformation($"Skipping duplicate record for {_schemaName}.{_tableName}.");
                            continue;
                        }

                        emit(change);

                        // Update last-hash checkpoint
                        lastHash = currentHash;
                        _checkpointStore.Put(_lastRecordHashKey, Encoding.UTF8.GetBytes(lastHash));
                    }

                    // Update LSN checkpoint if new changes arrived
                    if (newChanges.Any() && !string.IsNullOrEmpty(latestLsn))
                    {
                        _checkpointStore.Put(_checkpointKey, Encoding.UTF8.GetBytes(latestLsn));
                        LogInformation($"Updated LSN checkpoint to {latestLsn} for {_schemaName}.{_tableName}.");
                    }

                    // Reset back-off if we succeeded
                    backOffSeconds = 1;

                    // Sleep before the next poll
                    Thread.Sleep(_pollIntervalMs);
                }
                catch (Exception ex)
                {
                    LogError($"Error in Postgres CDC polling loop for {_schemaName}.{_tableName}.", ex);

                    // Exponential back-off
                    Thread.Sleep(TimeSpan.FromSeconds(backOffSeconds));
                    backOffSeconds = Math.Min(backOffSeconds * 2, _maxBackOffSeconds);
                }
            }
        }

        /// <summary>
        /// Reads the entire table once and emits each row as an Operation='InitialLoad'.
        /// </summary>
        private void RunInitialLoad(Action<PostgresRecord> emit)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            // For production usage, consider chunking or paging if the table is large.
            string sql = $@"SELECT * FROM ""{_schemaName}"".""{_tableName}"";";
            using var cmd = new NpgsqlCommand(sql, conn)
            {
                CommandTimeout = 180 // or as appropriate for large table scans
            };

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (_stopRequested)
                    break;

                var record = new PostgresRecord
                {
                    Operation = "InitialLoad",
                    Data = new Dictionary<string, object>(),
                    ChangeTime = DateTime.UtcNow
                };

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    object value = reader.GetValue(i);
                    record.Data[colName] = (value == DBNull.Value) ? null : value;
                }

                emit(record);
            }
        }

        /// <summary>
        /// Ensures that a publication and replication slot exist for this table, if requested.
        /// </summary>
        private void EnsureLogicalReplicationSetup()
        {
            LogInformation($"Ensuring logical replication setup for table {_schemaName}.{_tableName}...");

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            // 1. Adjust REPLICA IDENTITY if requested
            if (_replicaIdentity != ReplicaIdentityMode.None)
            {
                // Build the SQL statement for altering the table
                string replicaIdentitySql = _replicaIdentity switch
                {
                    ReplicaIdentityMode.Default =>
                        $@"ALTER TABLE ""{_schemaName}"".""{_tableName}"" REPLICA IDENTITY DEFAULT;",
                    ReplicaIdentityMode.Full =>
                        $@"ALTER TABLE ""{_schemaName}"".""{_tableName}"" REPLICA IDENTITY FULL;",
                    _ => null
                };

                if (!string.IsNullOrEmpty(replicaIdentitySql))
                {
                    using var alterCmd = new NpgsqlCommand(replicaIdentitySql, conn);
                    alterCmd.ExecuteNonQuery();
                    LogInformation($"Set table {_schemaName}.{_tableName} to REPLICA IDENTITY = {_replicaIdentity}.");
                }
            }

            // 2. Create publication if it doesn't exist
            try
            {
                string createPubSql = $@"
                    CREATE PUBLICATION ""{_publicationName}""
                    FOR TABLE ""{_schemaName}"".""{_tableName}"";
                ";
                using var pubCmd = new NpgsqlCommand(createPubSql, conn);
                pubCmd.ExecuteNonQuery();
                LogInformation($"Created publication '{_publicationName}' for {_schemaName}.{_tableName}.");
            }
            catch (PostgresException ex) when (ex.SqlState == "42710") // 42710 = duplicate_object
            {
                LogInformation($"Publication '{_publicationName}' already exists, skipping.");
            }

            // 3. Create replication slot if it doesn't exist
            try
            {
                string createSlotSql = $@"
                    SELECT * FROM pg_create_logical_replication_slot('{_slotName}', 'wal2json');
                ";
                using var slotCmd = new NpgsqlCommand(createSlotSql, conn);
                slotCmd.ExecuteNonQuery();
                LogInformation($"Created logical replication slot '{_slotName}' for {_schemaName}.{_tableName}.");
            }
            catch (PostgresException ex) when (ex.SqlState == "42710") // 42710 = duplicate_object
            {
                LogInformation($"Logical replication slot '{_slotName}' already exists, skipping.");
            }

            LogInformation($"Logical replication enabled for table {_schemaName}.{_tableName}. "
                         + $"Publication: {_publicationName}, Slot: {_slotName}");
        }

        /// <summary>
        /// Retrieves changes since the last known LSN using pg_logical_slot_get_changes(...).
        /// Returns the list of PostgresRecords and the highest LSN observed.
        /// </summary>
        private (List<PostgresRecord>, string) GetChangesSinceLastLsn(string lastLsn)
        {
            var changes = new List<PostgresRecord>();
            string newLastLsn = lastLsn;

            // For simplicity, ignoring lastLsn in the query and letting the slot's position track:
            var sql =
                  $@"SELECT lsn::text, xid::text, data
                     FROM pg_logical_slot_get_changes('{_slotName}', NULL, NULL,
                          'pretty-print', 'false',
                          'include-lsn', 'true',
                          'include-timestamp', 'true');";

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = 60 };
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (_stopRequested) break;

                string lsnString = reader.GetString(0);
                string xidString = reader.GetString(1);
                string json = reader.GetString(2);

                newLastLsn = lsnString; // track the highest LSN

                try
                {
                    var rootNode = JsonNode.Parse(json);
                    var changeArray = rootNode?["change"] as JsonArray;
                    if (changeArray == null || changeArray.Count == 0)
                        continue;

                    foreach (var changeObj in changeArray)
                    {
                        if (_stopRequested) break;

                        var kind = changeObj?["kind"]?.ToString()?.ToLowerInvariant() ?? "unknown";
                        var colNames = changeObj?["columnnames"] as JsonArray;
                        var colValues = changeObj?["columnvalues"] as JsonArray;

                        var record = new PostgresRecord
                        {
                            Operation = kind switch
                            {
                                "insert" => "INSERT",
                                "update" => "UPDATE",
                                "delete" => "DELETE",
                                _ => "UNKNOWN"
                            },
                            Data = new Dictionary<string, object>(),
                            ChangeTime = DateTime.UtcNow
                        };

                        // If REPLICA IDENTITY FULL is set, wal2json includes colNames/colValues for delete 
                        // Otherwise it might only have "oldkeys"
                        if (colNames != null && colValues != null)
                        {
                            for (int i = 0; i < colNames.Count; i++)
                            {
                                string name = colNames[i]?.ToString()!;
                                record.Data[name] = colValues[i]?.GetValue<object>()!;
                            }
                        }
                        else if (kind == "delete")
                        {
                            // Fallback to oldkeys if no colNames for DELETE
                            var oldKeys = changeObj?["oldkeys"];
                            var keyNames = oldKeys?["keynames"] as JsonArray;
                            var keyValues = oldKeys?["keyvalues"] as JsonArray;

                            if (keyNames != null && keyValues != null)
                            {
                                for (int i = 0; i < keyNames.Count; i++)
                                {
                                    string name = keyNames[i]?.ToString()!;
                                    record.Data[name] = keyValues[i]?.GetValue<object>()!;
                                }
                            }
                        }

                        changes.Add(record);
                    }
                }
                catch (Exception parseEx)
                {
                    LogError($"Failed to parse wal2json output for XID={xidString} at LSN={lsnString}. JSON: {json}.", parseEx);
                }
            }

            return (changes, newLastLsn);
        }

        /// <summary>
        /// Compute an MD5 hash from the PostgresRecord's Data dictionary,
        /// sorted by key for deterministic ordering.
        /// </summary>
        private string ComputeHash(PostgresRecord record)
        {
            var sb = new StringBuilder();
            foreach (var kv in record.Data.OrderBy(x => x.Key))
            {
                sb.Append(kv.Key).Append('=').Append(kv.Value ?? "null").Append(';');
            }

            using var md5 = MD5.Create();
            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            var hashBytes = md5.ComputeHash(bytes);
            return Convert.ToBase64String(hashBytes);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // free managed resources
                Stop();
            }
            _disposed = true;
        }

        // --------------------------------------------------------------------
        // LOGGING HELPERS: If _logger is null, we fall back to Console.WriteLine
        // --------------------------------------------------------------------
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

        private void LogError(string message, Exception ex = null)
        {
            if (_logger != null)
            {
                _logger.LogError(ex, message);
            }
            else
            {
                // Log with exception details on console if present
                if (ex != null)
                {
                    Console.WriteLine($"ERROR: {message}\n{ex}");
                }
                else
                {
                    Console.WriteLine($"ERROR: {message}");
                }
            }
        }
    }
}
