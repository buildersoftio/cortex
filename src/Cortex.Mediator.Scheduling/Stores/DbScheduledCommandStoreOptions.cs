using System;
using System.Text.RegularExpressions;

namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>
    /// Options for <see cref="DbScheduledCommandStore"/>.
    /// </summary>
    public sealed class DbScheduledCommandStoreOptions
    {
        private static readonly Regex IdentifierPattern = new(@"^[A-Za-z_][A-Za-z0-9_]{0,99}\z", RegexOptions.CultureInvariant);

        /// <summary>The table that holds scheduled commands. Default: <c>ScheduledCommands</c>.</summary>
        public string TableName { get; set; } = "ScheduledCommands";

        /// <summary>
        /// The schema of the table, or <c>null</c> for the connection's default schema.
        /// For SQLite this names an attached database.
        /// </summary>
        public string Schema { get; set; }

        /// <summary>Whether to create the table and its index on first use when they do not exist. Default: <c>true</c>.</summary>
        public bool AutoCreateTable { get; set; } = true;

        /// <summary>
        /// Throws <see cref="ArgumentException"/> unless the table and schema names are plain identifiers
        /// (a letter or underscore followed by up to 99 letters, digits or underscores).
        /// The names are written into SQL text, so nothing else is accepted.
        /// </summary>
        public void Validate()
        {
            if (TableName == null || !IdentifierPattern.IsMatch(TableName))
                throw new ArgumentException($"Table name '{TableName}' is not a valid identifier.", nameof(TableName));
            if (Schema != null && !IdentifierPattern.IsMatch(Schema))
                throw new ArgumentException($"Schema name '{Schema}' is not a valid identifier.", nameof(Schema));
        }
    }
}
