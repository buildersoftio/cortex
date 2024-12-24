using System.Collections.Generic;
using System.Linq;

namespace Cortex.States.Cassandra
{
    public class KeyspaceConfiguration
    {
        public string ReplicationStrategy { get; set; } = "SimpleStrategy";
        public int ReplicationFactor { get; set; } = 1;
        public Dictionary<string, string> ReplicationOptions { get; set; }
        public bool DurableWrites { get; set; } = true;

        public string GenerateCreateKeyspaceCql(string keyspaceName)
        {
            var replicationConfig = new Dictionary<string, string>
            {
                { "class", ReplicationStrategy }
            };

            if (ReplicationStrategy == "SimpleStrategy")
            {
                replicationConfig.Add("replication_factor", ReplicationFactor.ToString());
            }
            else if (ReplicationStrategy == "NetworkTopologyStrategy" && ReplicationOptions != null)
            {
                foreach (var option in ReplicationOptions)
                {
                    replicationConfig.Add(option.Key, option.Value);
                }
            }

            var replicationString = string.Join(", ",
                replicationConfig.Select(kv => $"'{kv.Key}': '{kv.Value}'"));

            return $@"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} 
                     WITH replication = {{{replicationString}}}
                     AND durable_writes = {DurableWrites.ToString().ToLower()}";
        }
    }
}
