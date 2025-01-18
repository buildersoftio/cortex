using System;

namespace Cortex.Streams.MongoDb
{
    public class MongoDbCDCSettings
    {
        /// <summary>
        /// Whether to perform an initial scan/load of the entire collection
        /// before starting the Change Stream.
        /// </summary>
        public bool DoInitialLoad { get; set; }

        /// <summary>
        /// For MongoDB, we use a Change Stream (which blocks). However, we may still
        /// use this interval as a minimal delay between reconnection attempts or
        /// to check if the operator was stopped.
        /// </summary>
        public TimeSpan Delay { get; set; }

        /// <summary>
        /// Maximum back-off in seconds if repeated errors occur while listening
        /// to the MongoDB Change Stream.
        /// </summary>
        public int MaxBackOffSeconds { get; set; }

        public MongoDbCDCSettings()
        {
            DoInitialLoad = true;

            // Typically not used for "polling" as we do streaming, but can be used
            // for a small wait loop to check cancellation or errors.
            Delay = TimeSpan.FromSeconds(3);
            MaxBackOffSeconds = 60;
        }
    }
}
