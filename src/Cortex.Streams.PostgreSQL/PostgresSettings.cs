using System;

namespace Cortex.Streams.PostgreSQL
{
    public class PostgresSettings
    {
        /// <summary>
        /// Whether to perform an initial table load before starting the CDC stream.
        /// </summary>
        public bool DoInitialLoad { get; set; }

        /// <summary>
        /// Interval at which we poll the replication slot for new changes (if not streaming).
        /// </summary>
        public TimeSpan PullInterval { get; set; }

        /// <summary>
        /// If true, will attempt to create the needed publication and logical slot if they don’t exist.
        /// </summary>
        public bool ConfigureCDCInServer { get; set; }

        /// <summary>
        /// Maximum wait time for back-off in seconds if an error repeatedly occurs.
        /// </summary>
        public int MaxBackOffSeconds { get; set; }

        /// <summary>
        /// Determines the REPLICA IDENTITY mode for the target table, if we are configuring CDC in the server.
        /// - None: Do not alter replica identity.
        /// - Default: Set REPLICA IDENTITY to DEFAULT.
        /// - Full: Set REPLICA IDENTITY FULL.
        /// </summary>
        public ReplicaIdentityMode ReplicaIdentity { get; set; }

        public PostgresSettings()
        {
            DoInitialLoad = true;
            PullInterval = TimeSpan.FromSeconds(3);
            ConfigureCDCInServer = false;
            MaxBackOffSeconds = 60;

            ReplicaIdentity = ReplicaIdentityMode.None; // default to no changes
        }
    }

    /// <summary>
    /// Represents possible settings for REPLICA IDENTITY on a PostgreSQL table.
    /// </summary>
    public enum ReplicaIdentityMode
    {
        /// <summary>
        /// Do not alter the table’s REPLICA IDENTITY; leave it as-is.
        /// </summary>
        None,

        /// <summary>
        /// Use the default replica identity, typically the primary key only.
        /// </summary>
        Default,

        /// <summary>
        /// Use REPLICA IDENTITY FULL, meaning deleted rows emit all columns, not just PK.
        /// </summary>
        Full
    }
}