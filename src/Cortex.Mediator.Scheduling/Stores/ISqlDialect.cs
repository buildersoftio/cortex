namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>
    /// The database-specific SQL used by <see cref="DbScheduledCommandStore"/>.
    /// Identifiers passed in are already validated as plain identifiers.
    /// </summary>
    public interface ISqlDialect
    {
        /// <summary>Quotes a plain identifier, for example <c>[Name]</c> or <c>"Name"</c>.</summary>
        string QuoteIdentifier(string identifier);

        /// <summary>
        /// Returns SQL that creates the table and its (Status, ExecuteAt) index when they do not exist.
        /// Columns: Id (string key, up to 64 characters), CommandType (string, up to 1024), Payload (text),
        /// ExecuteAt and CreatedAt (64-bit UTC ticks), Status and Attempts (32-bit integers),
        /// LastError (nullable text), LockedUntil (nullable 64-bit UTC ticks).
        /// </summary>
        /// <param name="schema">The schema, or <c>null</c> for the default.</param>
        /// <param name="table">The table name.</param>
        string GetCreateTableSql(string schema, string table);

        /// <summary>
        /// Returns a query selecting the Id of at most <c>@limit</c> claimable rows, ordered by ExecuteAt then Id:
        /// rows with Status 0 (pending) and ExecuteAt &lt;= <c>@now</c>, and rows with Status 1 (processing)
        /// and LockedUntil &lt;= <c>@now</c>.
        /// </summary>
        /// <param name="qualifiedTable">The quoted, schema-qualified table name.</param>
        string GetSelectDueIdsSql(string qualifiedTable);
    }
}
