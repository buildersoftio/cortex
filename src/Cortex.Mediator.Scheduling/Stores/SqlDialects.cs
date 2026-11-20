namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>Microsoft SQL Server dialect (use with Microsoft.Data.SqlClient).</summary>
    public sealed class SqlServerDialect : ISqlDialect
    {
        /// <inheritdoc />
        public string QuoteIdentifier(string identifier) => "[" + identifier + "]";

        /// <inheritdoc />
        public string GetCreateTableSql(string schema, string table)
        {
            var qualified = (schema == null ? string.Empty : QuoteIdentifier(schema) + ".") + QuoteIdentifier(table);
            var index = QuoteIdentifier("IX_" + table + "_Status_ExecuteAt");

            return
$@"IF OBJECT_ID(N'{qualified}', N'U') IS NULL
BEGIN
    CREATE TABLE {qualified} (
        [Id] NVARCHAR(64) NOT NULL PRIMARY KEY,
        [CommandType] NVARCHAR(1024) NOT NULL,
        [Payload] NVARCHAR(MAX) NOT NULL,
        [ExecuteAt] BIGINT NOT NULL,
        [CreatedAt] BIGINT NOT NULL,
        [Status] INT NOT NULL,
        [Attempts] INT NOT NULL,
        [LastError] NVARCHAR(MAX) NULL,
        [LockedUntil] BIGINT NULL
    );
    CREATE INDEX {index} ON {qualified} ([Status], [ExecuteAt]);
END";
        }

        /// <inheritdoc />
        public string GetSelectDueIdsSql(string qualifiedTable) =>
$@"SELECT TOP (@limit) [Id] FROM {qualifiedTable}
WHERE ([Status] = 0 AND [ExecuteAt] <= @now) OR ([Status] = 1 AND [LockedUntil] <= @now)
ORDER BY [ExecuteAt], [Id]";
    }

    /// <summary>PostgreSQL dialect (use with Npgsql).</summary>
    public sealed class PostgreSqlDialect : ISqlDialect
    {
        /// <inheritdoc />
        public string QuoteIdentifier(string identifier) => "\"" + identifier + "\"";

        /// <inheritdoc />
        public string GetCreateTableSql(string schema, string table)
        {
            var qualified = (schema == null ? string.Empty : QuoteIdentifier(schema) + ".") + QuoteIdentifier(table);
            var index = QuoteIdentifier("IX_" + table + "_Status_ExecuteAt");

            return
$@"CREATE TABLE IF NOT EXISTS {qualified} (
    ""Id"" VARCHAR(64) NOT NULL PRIMARY KEY,
    ""CommandType"" VARCHAR(1024) NOT NULL,
    ""Payload"" TEXT NOT NULL,
    ""ExecuteAt"" BIGINT NOT NULL,
    ""CreatedAt"" BIGINT NOT NULL,
    ""Status"" INTEGER NOT NULL,
    ""Attempts"" INTEGER NOT NULL,
    ""LastError"" TEXT NULL,
    ""LockedUntil"" BIGINT NULL
);
CREATE INDEX IF NOT EXISTS {index} ON {qualified} (""Status"", ""ExecuteAt"");";
        }

        /// <inheritdoc />
        public string GetSelectDueIdsSql(string qualifiedTable) =>
$@"SELECT ""Id"" FROM {qualifiedTable}
WHERE (""Status"" = 0 AND ""ExecuteAt"" <= @now) OR (""Status"" = 1 AND ""LockedUntil"" <= @now)
ORDER BY ""ExecuteAt"", ""Id""
LIMIT @limit";
    }

    /// <summary>SQLite dialect (use with Microsoft.Data.Sqlite).</summary>
    public sealed class SqliteDialect : ISqlDialect
    {
        /// <inheritdoc />
        public string QuoteIdentifier(string identifier) => "\"" + identifier + "\"";

        /// <inheritdoc />
        public string GetCreateTableSql(string schema, string table)
        {
            var prefix = schema == null ? string.Empty : QuoteIdentifier(schema) + ".";
            var qualified = prefix + QuoteIdentifier(table);
            var index = prefix + QuoteIdentifier("IX_" + table + "_Status_ExecuteAt");

            return
$@"CREATE TABLE IF NOT EXISTS {qualified} (
    ""Id"" TEXT NOT NULL PRIMARY KEY,
    ""CommandType"" TEXT NOT NULL,
    ""Payload"" TEXT NOT NULL,
    ""ExecuteAt"" INTEGER NOT NULL,
    ""CreatedAt"" INTEGER NOT NULL,
    ""Status"" INTEGER NOT NULL,
    ""Attempts"" INTEGER NOT NULL,
    ""LastError"" TEXT NULL,
    ""LockedUntil"" INTEGER NULL
);
CREATE INDEX IF NOT EXISTS {index} ON {QuoteIdentifier(table)} (""Status"", ""ExecuteAt"");";
        }

        /// <inheritdoc />
        public string GetSelectDueIdsSql(string qualifiedTable) =>
$@"SELECT ""Id"" FROM {qualifiedTable}
WHERE (""Status"" = 0 AND ""ExecuteAt"" <= @now) OR (""Status"" = 1 AND ""LockedUntil"" <= @now)
ORDER BY ""ExecuteAt"", ""Id""
LIMIT @limit";
    }
}
