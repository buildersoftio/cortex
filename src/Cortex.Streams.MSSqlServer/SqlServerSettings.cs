namespace Cortex.Streams.MSSqlServer
{
    public class SqlServerSettings
    {
        public bool DoInitialLoad { get; set; }
        public TimeSpan PullInterval { get; set; }
        public bool ConfigureCDCInServer { get; set; }

        public SqlServerSettings()
        {
            DoInitialLoad = true;
            PullInterval = TimeSpan.FromSeconds(3);
            ConfigureCDCInServer = false;
        }
    }
}
