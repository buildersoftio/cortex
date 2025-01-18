using Cortex.Streams.Operators;
using Microsoft.Extensions.Logging;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Cortex.States;

namespace Cortex.Streams.MongoDb
{
    /// <summary>
    /// A generic MongoDB CDC Source Operator that:
    /// 1. Optionally performs an initial collection load (deserializing docs into T).
    /// 2. Uses MongoDB Change Streams to capture inserts, updates, deletes, etc.
    /// 3. Stores the last resume token for correct restart.
    /// 4. Skips duplicate records using an MD5 hash of the serialized document.
    /// 5. Uses robust error handling and exponential back-off on failures.
    public class MongoDbCDCSourceOperator<T> : ISourceOperator<MongoDbRecord<T>>, IDisposable where T : new()
    {
        private readonly IMongoDatabase _database;
        private readonly string _collectionName;

        private readonly bool _doInitialLoad;
        private readonly int _pollIntervalMs;
        private readonly int _maxBackOffSeconds;

        private readonly IDataStore<string, byte[]> _checkpointStore;

        // Checkpoint keys
        private readonly string _checkpointKey;
        private readonly string _initialLoadCheckpointKey;
        private readonly string _lastRecordHashKey;

        // Collection reference
        private IMongoCollection<BsonDocument> _collection;

        // Thread control
        private Thread _pollingThread;
        private bool _stopRequested;
        private bool _disposed;

        // Optional logger
        private readonly ILogger<MongoDbCDCSourceOperator<T>> _logger;

        public MongoDbCDCSourceOperator(
            IMongoDatabase database,
            string collectionName,
            MongoDbCDCSettings mongoSettings = null,
            IDataStore<string, byte[]> checkpointStore = null,
            ILogger<MongoDbCDCSourceOperator<T>> logger = null)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            if (string.IsNullOrWhiteSpace(collectionName))
                throw new ArgumentException("Collection name cannot be null or empty.", nameof(collectionName));

            mongoSettings ??= new MongoDbCDCSettings();
            _checkpointStore = checkpointStore
                ?? new InMemoryStateStore<string, byte[]>($"{database.DatabaseNamespace.DatabaseName}.{collectionName}.STORE");

            _collectionName = collectionName;
            _doInitialLoad = mongoSettings.DoInitialLoad;
            _pollIntervalMs = (int)mongoSettings.Delay.TotalMilliseconds;
            _maxBackOffSeconds = mongoSettings.MaxBackOffSeconds;

            // Build checkpoint keys
            var dbName = _database.DatabaseNamespace.DatabaseName;
            _checkpointKey = $"{dbName}.{collectionName}.CDC.ResumeToken";
            _initialLoadCheckpointKey = $"{dbName}.{collectionName}.INITIAL_LOAD_DONE";
            _lastRecordHashKey = $"{dbName}.{collectionName}.CDC.LAST_HASH";

            _logger = logger;
        }

        /// <summary>
        /// Start the operator. Sets up the collection, performs optional initial load,
        /// and spawns a thread that listens to the MongoDB change stream.
        /// </summary>
        public void Start(Action<MongoDbRecord<T>> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));

            var dbName = _database.DatabaseNamespace.DatabaseName;
            LogInformation($"Starting MongoDB CDC operator for {dbName}.{_collectionName}...");

            // 1. Select the collection
            _collection = _database.GetCollection<BsonDocument>(_collectionName);

            // 2. Initial Load (once)
            if (_doInitialLoad && _checkpointStore.Get(_initialLoadCheckpointKey) == null)
            {
                LogInformation($"Performing initial load for {dbName}.{_collectionName}...");
                RunInitialLoad(emit);
                _checkpointStore.Put(_initialLoadCheckpointKey, new byte[] { 0x01 });
                LogInformation($"Initial load completed for {dbName}.{_collectionName}.");
            }
            else
            {
                LogInformation($"Skipping initial load for {dbName}.{_collectionName} (already done or disabled).");
            }

            // 3. Check existing resume token
            var lastResumeTokenBytes = _checkpointStore.Get(_checkpointKey);
            if (lastResumeTokenBytes == null)
            {
                LogInformation($"No existing resume token found for {dbName}.{_collectionName}. Will start fresh.");
            }
            else
            {
                LogInformation($"Found existing resume token for {dbName}.{_collectionName}.");
            }

            // 4. Spin up background thread
            _stopRequested = false;
            _pollingThread = new Thread(() => PollCdcChanges(emit))
            {
                IsBackground = true,
                Name = $"MongoCdcPolling_{dbName}_{_collectionName}"
            };
            _pollingThread.Start();
        }

        /// <summary>
        /// Gracefully stops the operator.
        /// </summary>
        public void Stop()
        {
            var dbName = _database.DatabaseNamespace.DatabaseName;
            LogInformation($"Stop requested for MongoDB CDC operator {dbName}.{_collectionName}.");
            _stopRequested = true;
            _pollingThread?.Join();
            LogInformation($"MongoDB CDC operator stopped for {dbName}.{_collectionName}.");
        }

        /// <summary>
        /// The main CDC polling logic with a change stream + exponential back-off on errors.
        /// </summary>
        private void PollCdcChanges(Action<MongoDbRecord<T>> emit)
        {
            var dbName = _database.DatabaseNamespace.DatabaseName;
            int backOffSeconds = 1;

            while (!_stopRequested)
            {
                try
                {
                    // 1. Load the last resume token
                    BsonDocument resumeToken = null;
                    var lastResumeTokenBytes = _checkpointStore.Get(_checkpointKey);
                    if (lastResumeTokenBytes != null)
                    {
                        var tokenJson = Encoding.UTF8.GetString(lastResumeTokenBytes);
                        resumeToken = BsonDocument.Parse(tokenJson);
                    }

                    // 2. Configure the change stream
                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        ResumeAfter = resumeToken
                    };

                    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>();
                    using var cursor = _collection.Watch(pipeline, options);

                    LogInformation($"MongoDB Change Stream opened for {dbName}.{_collectionName}.");

                    // 3. Check last-record-hash to avoid duplicates
                    var lastHashBytes = _checkpointStore.Get(_lastRecordHashKey);
                    string lastHash = lastHashBytes == null ? null : Encoding.UTF8.GetString(lastHashBytes);

                    // 4. Read from the cursor
                    foreach (var change in cursor.ToEnumerable())
                    {
                        if (_stopRequested) break;

                        // (a) Update the resume token checkpoint
                        var currentResumeToken = change.ResumeToken?.AsBsonDocument;
                        if (currentResumeToken != null)
                        {
                            var tokenString = currentResumeToken.ToJson();
                            _checkpointStore.Put(_checkpointKey, Encoding.UTF8.GetBytes(tokenString));
                        }

                        // (b) Convert this to a typed record
                        var record = ConvertChangeToRecord(change);

                        // (c) Compute a hash for dedup (based on JSON)
                        var docForHash = change.FullDocument?.ToJson()
                                         ?? change.DocumentKey?.ToJson()
                                         ?? string.Empty;
                        var currentHash = ComputeMd5Base64(docForHash);

                        // (d) Check if it's a duplicate
                        if (currentHash == lastHash)
                        {
                            LogInformation($"Skipping duplicate record for {dbName}.{_collectionName}.");
                            continue;
                        }

                        // (e) Emit and update the last hash
                        emit(record);
                        lastHash = currentHash;
                        _checkpointStore.Put(_lastRecordHashKey, Encoding.UTF8.GetBytes(lastHash));
                    }

                    // If no changes come in, we still check the stop flag periodically
                    Thread.Sleep(_pollIntervalMs);

                    // Reset back-off if we got here without exceptions
                    backOffSeconds = 1;
                }
                catch (Exception ex)
                {
                    LogError($"Error in MongoDB CDC polling loop for {dbName}.{_collectionName}.", ex);
                    Thread.Sleep(TimeSpan.FromSeconds(backOffSeconds));
                    backOffSeconds = Math.Min(backOffSeconds * 2, _maxBackOffSeconds);
                }
            }
        }

        /// <summary>
        /// Loads the entire collection once, deserializing each document into T, 
        /// and emitting a record with Operation="InitialLoad".
        /// </summary>
        private void RunInitialLoad(Action<MongoDbRecord<T>> emit)
        {
            var filter = Builders<BsonDocument>.Filter.Empty;
            var cursor = _collection.Find(filter).ToCursor();

            while (cursor.MoveNext())
            {
                foreach (var doc in cursor.Current)
                {
                    if (_stopRequested) break;

                    T typedDoc = BsonSerializer.Deserialize<T>(doc);

                    var record = new MongoDbRecord<T>
                    {
                        Operation = "InitialLoad",
                        Data = typedDoc,
                        ChangeTime = DateTime.UtcNow
                    };

                    emit(record);
                }
                if (_stopRequested) break;
            }
        }

        /// <summary>
        /// Converts a change stream event into a <see cref="MongoDbRecord{T}"/>.
        /// </summary>
        private MongoDbRecord<T> ConvertChangeToRecord(ChangeStreamDocument<BsonDocument> change)
        {
            var record = new MongoDbRecord<T>
            {
                ChangeTime = DateTime.UtcNow,
            };

            switch (change.OperationType)
            {
                case ChangeStreamOperationType.Insert:
                    record.Operation = "INSERT";
                    record.Data = change.FullDocument != null
                        ? BsonSerializer.Deserialize<T>(change.FullDocument)
                        : default;
                    break;

                case ChangeStreamOperationType.Update:
                    record.Operation = "UPDATE";
                    record.Data = change.FullDocument != null
                        ? BsonSerializer.Deserialize<T>(change.FullDocument)
                        : default;
                    break;

                case ChangeStreamOperationType.Replace:
                    record.Operation = "REPLACE";
                    record.Data = change.FullDocument != null
                        ? BsonSerializer.Deserialize<T>(change.FullDocument)
                        : default;
                    break;

                case ChangeStreamOperationType.Delete:
                    record.Operation = "DELETE";
                    // For deletes, FullDocument is normally null.
                    // If you only have the _id, you can build a partial T or just leave it as default.
                    record.Data = default;
                    break;

                default:
                    // Could handle drop, rename, invalidate, etc.
                    record.Operation = change.OperationType.ToString().ToUpperInvariant();
                    record.Data = default;
                    break;
            }

            return record;
        }

        /// <summary>
        /// Computes an MD5 hash of the given string, returns Base64.
        /// </summary>
        private static string ComputeMd5Base64(string raw)
        {
            using var md5 = MD5.Create();
            var bytes = Encoding.UTF8.GetBytes(raw);
            var hash = md5.ComputeHash(bytes);
            return Convert.ToBase64String(hash);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                Stop();
            }
            _disposed = true;
        }

        // --------------------------------------------------------------------
        // LOGGING HELPERS
        // --------------------------------------------------------------------
        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger?.LogInformation(message);
            else
                Console.WriteLine(message);
        }

        private void LogError(string message, Exception ex = null)
        {
            if (_logger != null)
            {
                _logger.LogError(ex, message);
            }
            else
            {
                Console.WriteLine(ex != null
                    ? $"ERROR: {message}\n{ex}"
                    : $"ERROR: {message}");
            }
        }
    }
}
