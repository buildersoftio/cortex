namespace Cortex.States.ClickHouse
{
    public enum ClickHouseTableEngine
    {
        MergeTree,
        ReplacingMergeTree,
    }

    public class ClickHouseConfiguration
    {
        public ClickHouseTableEngine TableEngine { get; set; } = ClickHouseTableEngine.MergeTree;

        /// <summary>
        /// Additional raw text appended to the engine definition, e.g. PARTITION BY, ORDER BY clauses if needed.
        /// For example: "ORDER BY key" or "ORDER BY (key) PRIMARY KEY (key) SETTINGS index_granularity = 8192"
        /// </summary>
        public string EngineModifiers { get; set; } = "ORDER BY key";

        /// <summary>
        /// If true, the table creation code will attempt to create or alter columns to match schema.
        /// If false, throws if the table or columns do not exist.
        /// </summary>
        public bool CreateOrUpdateTableSchema { get; set; } = true;
    }
}
