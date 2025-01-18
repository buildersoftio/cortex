using System;
using System.Collections.Generic;

namespace Cortex.Streams.MSSqlServer
{
    /// <summary>
    /// Represents a generic CDC record, carrying enough information
    /// to understand what changed and how.
    /// </summary>
    public class SqlServerRecord
    {
        /// <summary>
        /// The operation name or type, e.g. 'INSERT', 'UPDATE', 'DELETE'.
        /// </summary>
        public string Operation { get; set; }  // Insert, Update, Delete, etc.


        /// <summary>
        /// The primary key or other columns in the changed record, 
        /// serialized for demonstration.
        /// </summary>
        public Dictionary<string, object> Data { get; set; } // Column names -> values

        /// <summary>
        /// Timestamp or time of the record, if needed.
        /// </summary>
        public DateTime ChangeTime { get; set; }
    }
}
