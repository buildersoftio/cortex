using System;
using System.Collections.Generic;

namespace Cortex.Streams.MongoDb
{
    /// <summary>
    /// Represents a generic CDC record for MongoDB.
    /// </summary>
    public class MongoDbRecord
    {
        /// <summary>
        /// The operation type: 'INSERT', 'UPDATE', 'DELETE', 'REPLACE', or 'InitialLoad'.
        /// </summary>
        public string Operation { get; set; }

        /// <summary>
        /// The document data (fields and values) after the operation (for insert/update/replace).
        /// For deletes, this may contain only the _id or other keys available before deletion.
        /// </summary>
        public Dictionary<string, object> Data { get; set; }

        /// <summary>
        /// The approximate time (UTC) when this change was captured or emitted.
        /// </summary>
        public DateTime ChangeTime { get; set; }
    }
    public class MongoDbRecord<T>
    {
        /// <summary>
        /// The operation type: 'INSERT', 'UPDATE', 'DELETE', 'REPLACE', or 'InitialLoad'.
        /// </summary>
        public string Operation { get; set; }

        /// <summary>
        /// The document data, deserialized into type T.
        /// For deletes, this may be default(T) if the document no longer exists.
        /// </summary>
        public T Data { get; set; }

        /// <summary>
        /// The approximate time (UTC) when this change was captured or emitted.
        /// </summary>
        public DateTime ChangeTime { get; set; }
    }
}
