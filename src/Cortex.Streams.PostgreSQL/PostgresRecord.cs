namespace Cortex.Streams.PostgreSQL
{
    /// <summary>
    /// Represents a generic CDC record for PostgreSQL.
    /// </summary>
    public class PostgresRecord
    {
        /// <summary>
        /// The operation name or type, e.g. 'INSERT', 'UPDATE', 'DELETE', 'InitialLoad'.
        /// </summary>
        public string Operation { get; set; }

        /// <summary>
        /// The primary key or other columns in the changed record, 
        /// as a dictionary of column names to values.
        /// </summary>
        public Dictionary<string, object> Data { get; set; }

        /// <summary>
        /// The approximate time of this change or emission.
        /// </summary>
        public DateTime ChangeTime { get; set; }
    }
}
