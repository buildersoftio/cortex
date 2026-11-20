using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>
    /// A durable store that keeps scheduled commands in a database table, through any ADO.NET provider.
    /// Several processes may share the table: each command is claimed with a conditional UPDATE,
    /// so only one processor at a time owns it. Times are stored as UTC ticks.
    /// All SQL uses parameters; the table and schema names are validated identifiers.
    /// </summary>
    public sealed class DbScheduledCommandStore : IScheduledCommandStore
    {
        private const int StatusPending = (int)ScheduledCommandStatus.Pending;
        private const int StatusProcessing = (int)ScheduledCommandStatus.Processing;
        private const int StatusCompleted = (int)ScheduledCommandStatus.Completed;
        private const int StatusFailed = (int)ScheduledCommandStatus.Failed;
        private const int StatusCancelled = (int)ScheduledCommandStatus.Cancelled;

        private readonly Func<DbConnection> _connectionFactory;
        private readonly ISqlDialect _dialect;
        private readonly DbScheduledCommandStoreOptions _options;
        private readonly string _table;
        private readonly string _columns;
        private readonly SemaphoreSlim _initLock = new(1, 1);
        private volatile bool _initialized;

        /// <summary>Creates a store.</summary>
        /// <param name="connectionFactory">Creates a new, unopened connection for each operation.</param>
        /// <param name="dialect">The SQL dialect of the database.</param>
        /// <param name="options">Table options; defaults are used when <c>null</c>.</param>
        public DbScheduledCommandStore(Func<DbConnection> connectionFactory, ISqlDialect dialect, DbScheduledCommandStoreOptions options = null)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _options = options ?? new DbScheduledCommandStoreOptions();
            _options.Validate();

            _table = (_options.Schema == null ? string.Empty : _dialect.QuoteIdentifier(_options.Schema) + ".")
                     + _dialect.QuoteIdentifier(_options.TableName);
            _columns = string.Join(", ", Q("Id"), Q("CommandType"), Q("Payload"), Q("ExecuteAt"), Q("CreatedAt"),
                Q("Status"), Q("Attempts"), Q("LastError"), Q("LockedUntil"));
            _initialized = !_options.AutoCreateTable;
        }

        /// <summary>Creates the table and its index if they do not exist.</summary>
        public async Task EnsureTableAsync(CancellationToken ct = default)
        {
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCreateTableSql(_options.Schema, _options.TableName);
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task AddAsync(ScheduledCommand command, CancellationToken ct = default)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));
            if (string.IsNullOrWhiteSpace(command.Id))
                throw new ArgumentException("The scheduled command must have an id.", nameof(command));

            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            using var db = connection.CreateCommand();
            db.CommandText = $"INSERT INTO {_table} ({_columns}) VALUES (@id, @commandType, @payload, @executeAt, @createdAt, @status, @attempts, @lastError, @lockedUntil)";
            AddParameter(db, "@id", command.Id, DbType.String);
            AddParameter(db, "@commandType", command.CommandType, DbType.String);
            AddParameter(db, "@payload", command.Payload, DbType.String);
            AddParameter(db, "@executeAt", ToTicks(command.ExecuteAt), DbType.Int64);
            AddParameter(db, "@createdAt", ToTicks(command.CreatedAt), DbType.Int64);
            AddParameter(db, "@status", (int)command.Status, DbType.Int32);
            AddParameter(db, "@attempts", command.Attempts, DbType.Int32);
            AddParameter(db, "@lastError", command.LastError, DbType.String);
            AddParameter(db, "@lockedUntil", command.LockedUntil.HasValue ? ToTicks(command.LockedUntil.Value) : (long?)null, DbType.Int64);
            await db.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<ScheduledCommand> GetAsync(string id, CancellationToken ct = default)
        {
            if (id == null)
                return null;

            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            return await ReadByIdAsync(connection, id, ct).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<bool> CancelAsync(string id, CancellationToken ct = default)
        {
            if (id == null)
                return false;

            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            using var db = connection.CreateCommand();
            db.CommandText = $"UPDATE {_table} SET {Q("Status")} = @cancelled WHERE {Q("Id")} = @id AND {Q("Status")} = @pending";
            AddParameter(db, "@cancelled", StatusCancelled, DbType.Int32);
            AddParameter(db, "@id", id, DbType.String);
            AddParameter(db, "@pending", StatusPending, DbType.Int32);
            return await db.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyList<ScheduledCommand>> ClaimDueAsync(DateTimeOffset now, int maxCount, TimeSpan lockDuration, CancellationToken ct = default)
        {
            if (maxCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxCount), "The batch size must be positive.");

            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);

            var nowTicks = ToTicks(now);
            var candidates = new List<string>();
            using (var select = connection.CreateCommand())
            {
                select.CommandText = _dialect.GetSelectDueIdsSql(_table);
                AddParameter(select, "@now", nowTicks, DbType.Int64);
                AddParameter(select, "@limit", maxCount, DbType.Int32);
                using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    candidates.Add(Convert.ToString(reader.GetValue(0)));
            }

            var claimed = new List<ScheduledCommand>(candidates.Count);
            foreach (var id in candidates)
            {
                // Stop claiming on cancellation, but return what is already claimed so no row is stranded.
                if (ct.IsCancellationRequested)
                    break;

                using (var claim = connection.CreateCommand())
                {
                    claim.CommandText =
                        $"UPDATE {_table} SET {Q("Status")} = @processing, {Q("LockedUntil")} = @lockedUntil, {Q("Attempts")} = {Q("Attempts")} + 1 " +
                        $"WHERE {Q("Id")} = @id AND (({Q("Status")} = @pending AND {Q("ExecuteAt")} <= @now) " +
                        $"OR ({Q("Status")} = @processing AND {Q("LockedUntil")} <= @now))";
                    AddParameter(claim, "@processing", StatusProcessing, DbType.Int32);
                    AddParameter(claim, "@lockedUntil", ToTicks(now + lockDuration), DbType.Int64);
                    AddParameter(claim, "@id", id, DbType.String);
                    AddParameter(claim, "@pending", StatusPending, DbType.Int32);
                    AddParameter(claim, "@now", nowTicks, DbType.Int64);

                    if (await claim.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false) != 1)
                        continue;
                }

                // The row is ours now; read it back even if cancellation was requested, so it is not stranded unreported.
                var command = await ReadByIdAsync(connection, id, CancellationToken.None).ConfigureAwait(false);
                if (command != null)
                    claimed.Add(command);
            }

            return claimed;
        }

        /// <inheritdoc />
        public async Task MarkCompletedAsync(string id, CancellationToken ct = default)
        {
            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            using var db = connection.CreateCommand();
            db.CommandText = $"UPDATE {_table} SET {Q("Status")} = @completed, {Q("LockedUntil")} = NULL WHERE {Q("Id")} = @id";
            AddParameter(db, "@completed", StatusCompleted, DbType.Int32);
            AddParameter(db, "@id", id, DbType.String);
            await db.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task MarkFailedAsync(string id, string error, DateTimeOffset? retryAt, CancellationToken ct = default)
        {
            await EnsureInitializedAsync(ct).ConfigureAwait(false);
            using var connection = await OpenAsync(ct).ConfigureAwait(false);
            using var db = connection.CreateCommand();

            if (retryAt.HasValue)
            {
                db.CommandText =
                    $"UPDATE {_table} SET {Q("Status")} = @pending, {Q("ExecuteAt")} = @retryAt, {Q("LastError")} = @error, {Q("LockedUntil")} = NULL " +
                    $"WHERE {Q("Id")} = @id";
                AddParameter(db, "@pending", StatusPending, DbType.Int32);
                AddParameter(db, "@retryAt", ToTicks(retryAt.Value), DbType.Int64);
            }
            else
            {
                db.CommandText =
                    $"UPDATE {_table} SET {Q("Status")} = @failed, {Q("LastError")} = @error, {Q("LockedUntil")} = NULL WHERE {Q("Id")} = @id";
                AddParameter(db, "@failed", StatusFailed, DbType.Int32);
            }

            AddParameter(db, "@error", error, DbType.String);
            AddParameter(db, "@id", id, DbType.String);
            await db.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        private async Task<ScheduledCommand> ReadByIdAsync(DbConnection connection, string id, CancellationToken ct)
        {
            using var db = connection.CreateCommand();
            db.CommandText = $"SELECT {_columns} FROM {_table} WHERE {Q("Id")} = @id";
            AddParameter(db, "@id", id, DbType.String);

            using var reader = await db.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return null;

            return new ScheduledCommand
            {
                Id = Convert.ToString(reader.GetValue(0)),
                CommandType = Convert.ToString(reader.GetValue(1)),
                Payload = Convert.ToString(reader.GetValue(2)),
                ExecuteAt = FromTicks(Convert.ToInt64(reader.GetValue(3))),
                CreatedAt = FromTicks(Convert.ToInt64(reader.GetValue(4))),
                Status = (ScheduledCommandStatus)Convert.ToInt32(reader.GetValue(5)),
                Attempts = Convert.ToInt32(reader.GetValue(6)),
                LastError = reader.IsDBNull(7) ? null : Convert.ToString(reader.GetValue(7)),
                LockedUntil = reader.IsDBNull(8) ? null : FromTicks(Convert.ToInt64(reader.GetValue(8)))
            };
        }

        private async Task EnsureInitializedAsync(CancellationToken ct)
        {
            if (_initialized)
                return;

            await _initLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_initialized)
                    return;

                await EnsureTableAsync(ct).ConfigureAwait(false);
                _initialized = true;
            }
            finally
            {
                _initLock.Release();
            }
        }

        private async Task<DbConnection> OpenAsync(CancellationToken ct)
        {
            var connection = _connectionFactory()
                ?? throw new InvalidOperationException("The connection factory returned null.");
            try
            {
                await connection.OpenAsync(ct).ConfigureAwait(false);
                return connection;
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        private string Q(string identifier) => _dialect.QuoteIdentifier(identifier);

        private static void AddParameter(DbCommand command, string name, object value, DbType type)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = type;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        private static long ToTicks(DateTimeOffset value) => value.UtcTicks;

        private static DateTimeOffset FromTicks(long ticks) => new(ticks, TimeSpan.Zero);
    }
}
