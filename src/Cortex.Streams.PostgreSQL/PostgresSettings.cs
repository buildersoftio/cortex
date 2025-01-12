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

        public PostgresSettings()
        {
            DoInitialLoad = true;
            PullInterval = TimeSpan.FromSeconds(3);
            ConfigureCDCInServer = false;
            MaxBackOffSeconds = 60;
        }
    }
}