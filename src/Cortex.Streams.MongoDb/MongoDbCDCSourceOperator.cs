using Cortex.States;
using Cortex.Streams.Operators;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Cortex.Streams.MongoDb
{
    /// <summary>
    /// MongoDB CDC Source Operator that:
    /// 1. Optionally performs an initial collection load if requested.
    /// 2. Uses MongoDB Change Streams to capture inserts, updates, deletes, and replaces.
    /// 3. Stores the last resume token to allow restart from the correct position.
    /// 4. Skips duplicate records using a hash checkpoint.
    /// 5. Uses robust error handling and exponential back-off on failures.
    /// </summary>
    public class MongoDbCDCSourceOperator : ISourceOperator<MongoDbRecord>, IDisposable
    {
        private readonly IMongoDatabase _database;
        private readonly string _collectionName;

        private readonly bool _doInitialLoad;
        private readonly int _delayMs;
        private readonly int _maxBackOffSeconds;

        private readonly IDataStore<string, byte[]> _checkpointStore;

        // Keys for checkpoint store
        private readonly string _checkpointKey;
        private readonly string _initialLoadCheckpointKey;
        private readonly string _lastRecordHashKey;

        private IMongoCollection<BsonDocument> _collection;

        // Thread & cancellation
        private Thread _pollingThread;
        private bool _stopRequested;
        private bool _disposed;

        // Optional logger
        private readonly ILogger<MongoDbCDCSourceOperator> _logger;


        public MongoDbCDCSourceOperator(IMongoDatabase database,
            string collectionName,
            MongoDbCDCSettings mongoSettings = null,
            IDataStore<string, byte[]> checkpointStore = null,
            ILogger<MongoDbCDCSourceOperator> logger = null)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));
            if (string.IsNullOrWhiteSpace(collectionName))
                throw new ArgumentException("Collection name cannot be null or empty.", nameof(collectionName));

            mongoSettings ??= new MongoDbCDCSettings();

            // If no checkpoint store is provided, default to an in-memory store.
            _checkpointStore = checkpointStore
                ?? new InMemoryStateStore<string, byte[]>($"{database.DatabaseNamespace.DatabaseName}.{collectionName}.STORE");

            _database = database;
            _collectionName = collectionName;

            _doInitialLoad = mongoSettings.DoInitialLoad;
            _delayMs = (int)mongoSettings.Delay.TotalMilliseconds;
            _maxBackOffSeconds = mongoSettings.MaxBackOffSeconds;

            // Define checkpoint keys
            var dbName = database.DatabaseNamespace.DatabaseName;
            _checkpointKey = $"{dbName}.{collectionName}.CDC.ResumeToken";
            _initialLoadCheckpointKey = $"{dbName}.{collectionName}.INITIAL_LOAD_DONE";
            _lastRecordHashKey = $"{dbName}.{collectionName}.CDC.LAST_HASH";

            _logger = logger;
        }

        /// <summary>
        /// Start the operator. Uses the provided IMongoDatabase to set up the collection,
        /// performs optional initial load, and launches the background thread
        /// that reads from the change stream.
        /// </summary>
        public void Start(Action<MongoDbRecord> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));

            var dbName = _database.DatabaseNamespace.DatabaseName;
            LogInformation($"Starting MongoDB CDC operator for {dbName}.{_collectionName}...");

            // 1. Select the collection
            _collection = _database.GetCollection<BsonDocument>(_collectionName);

            // 2. Perform initial load if needed
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

            // 3. Check if we have an existing resume token
            var lastResumeTokenBytes = _checkpointStore.Get(_checkpointKey);
            if (lastResumeTokenBytes == null)
            {
                LogInformation($"No existing resume token found for {dbName}.{_collectionName}. Will start fresh.");
            }
            else
            {
                LogInformation($"Found existing resume token for {dbName}.{_collectionName}.");
            }

            // 4. Spin up the background polling (streaming) thread
            _stopRequested = false;
            _pollingThread = new Thread(() => PollCdcChanges(emit))
            {
                IsBackground = true,
                Name = $"MongoCdcPolling_{dbName}_{_collectionName}"
            };
            _pollingThread.Start();
        }

        /// <summary>
        /// Stops the operator gracefully.
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
        /// The main CDC logic that connects to the MongoDB Change Stream and reads events.
        /// Includes exponential back-off if errors occur.
        /// </summary>
        private void PollCdcChanges(Action<MongoDbRecord> emit)
        {
            var dbName = _database.DatabaseNamespace.DatabaseName;
            int backOffSeconds = 1;

            while (!_stopRequested)
            {
                try
                {
                    // 1. Retrieve the last resume token from checkpoint store
                    BsonDocument resumeToken = null;
                    var lastResumeTokenBytes = _checkpointStore.Get(_checkpointKey);
                    if (lastResumeTokenBytes != null)
                    {
                        var json = Encoding.UTF8.GetString(lastResumeTokenBytes);
                        resumeToken = BsonDocument.Parse(json);
                    }

                    // 2. Create a change stream with optional resume token
                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        ResumeAfter = resumeToken
                    };

                    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>();

                    using var cursor = _collection.Watch(pipeline, options);
                    LogInformation($"MongoDB Change Stream opened for {dbName}.{_collectionName}.");

                    // 3. Retrieve last-record-hash to skip duplicates
                    var lastHashBytes = _checkpointStore.Get(_lastRecordHashKey);
                    string lastHash = lastHashBytes == null ? null : Encoding.UTF8.GetString(lastHashBytes);

                    // 4. Read changes
                    foreach (var change in cursor.ToEnumerable())
                    {
                        if (_stopRequested) break;

                        // Update resume token checkpoint
                        var currentResumeToken = change.ResumeToken?.AsBsonDocument;
                        if (currentResumeToken != null)
                        {
                            var currentResumeTokenString = currentResumeToken.ToJson();
                            _checkpointStore.Put(_checkpointKey, Encoding.UTF8.GetBytes(currentResumeTokenString));
                        }

                        // Convert the change stream document to a MongoDbRecord
                        var record = ConvertChangeToRecord(change);

                        // Deduplicate
                        var currentHash = ComputeHash(record);
                        if (currentHash == lastHash)
                        {
                            LogInformation($"Skipping duplicate record for {dbName}.{_collectionName}.");
                            continue;
                        }

                        // Emit the record
                        emit(record);

                        // Update lastHash checkpoint
                        lastHash = currentHash;
                        _checkpointStore.Put(_lastRecordHashKey, Encoding.UTF8.GetBytes(lastHash));
                    }

                    Thread.Sleep(_delayMs);
                    // Reset back-off if we successfully processed
                    backOffSeconds = 1;
                }
                catch (Exception ex)
                {
                    LogError($"Error in MongoDB CDC polling loop for {dbName}.{_collectionName}.", ex);

                    // Exponential back-off
                    Thread.Sleep(TimeSpan.FromSeconds(backOffSeconds));
                    backOffSeconds = Math.Min(backOffSeconds * 2, _maxBackOffSeconds);
                }
            }
        }

        /// <summary>
        /// Reads the entire collection once and emits each document as Operation = 'InitialLoad'.
        /// For very large collections, consider chunking/paging to avoid memory issues.
        /// </summary>
        private void RunInitialLoad(Action<MongoDbRecord> emit)
        {
            var filter = Builders<BsonDocument>.Filter.Empty;
            var cursor = _collection.Find(filter).ToCursor();

            while (cursor.MoveNext())
            {
                foreach (var doc in cursor.Current)
                {
                    if (_stopRequested) break;

                    var record = new MongoDbRecord
                    {
                        Operation = "InitialLoad",
                        Data = BsonDocumentToDictionary(doc),
                        ChangeTime = DateTime.UtcNow
                    };

                    emit(record);
                }
                if (_stopRequested) break;
            }
        }

        /// <summary>
        /// Converts a change stream document to a <see cref="MongoDbRecord"/>.
        /// </summary>
        private MongoDbRecord ConvertChangeToRecord(ChangeStreamDocument<BsonDocument> change)
        {
            var record = new MongoDbRecord
            {
                Data = new Dictionary<string, object>(),
                ChangeTime = DateTime.UtcNow
            };

            switch (change.OperationType)
            {
                case ChangeStreamOperationType.Insert:
                    record.Operation = "INSERT";
                    record.Data = BsonDocumentToDictionary(change.FullDocument);
                    break;

                case ChangeStreamOperationType.Update:
                    record.Operation = "UPDATE";
                    record.Data = BsonDocumentToDictionary(change.FullDocument);
                    break;

                case ChangeStreamOperationType.Replace:
                    record.Operation = "REPLACE";
                    record.Data = BsonDocumentToDictionary(change.FullDocument);
                    break;

                case ChangeStreamOperationType.Delete:
                    record.Operation = "DELETE";
                    // DocumentKey typically has the _id
                    if (change.DocumentKey != null)
                    {
                        var docKeyDict = BsonDocumentToDictionary(change.DocumentKey);
                        foreach (var kv in docKeyDict)
                        {
                            record.Data[kv.Key] = kv.Value;
                        }
                    }
                    break;

                default:
                    record.Operation = change.OperationType.ToString().ToUpperInvariant();
                    break;
            }

            return record;
        }

        /// <summary>
        /// Helper to convert a BsonDocument to a Dictionary<string, object>.
        /// </summary>
        private Dictionary<string, object> BsonDocumentToDictionary(BsonDocument doc)
        {
            if (doc == null) return new Dictionary<string, object>();

            var dict = new Dictionary<string, object>();
            foreach (var element in doc.Elements)
            {
                dict[element.Name] = BsonValueToNative(element.Value);
            }
            return dict;
        }

        /// <summary>
        /// Recursively converts a BsonValue to a .NET object (e.g. string, int, nested dictionaries, etc.).
        /// </summary>
        private object BsonValueToNative(BsonValue value)
        {
            if (value == null || value.IsBsonNull) return null;

            switch (value.BsonType)
            {
                case BsonType.Document:
                    return BsonDocumentToDictionary(value.AsBsonDocument);

                case BsonType.Array:
                    var list = new List<object>();
                    foreach (var item in value.AsBsonArray)
                    {
                        list.Add(BsonValueToNative(item));
                    }
                    return list;

                case BsonType.Boolean: return value.AsBoolean;
                case BsonType.DateTime: return value.ToUniversalTime();
                case BsonType.Double: return value.AsDouble;
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Decimal128: return Convert.ToDecimal(value.AsDecimal128);
                case BsonType.String: return value.AsString;
                case BsonType.ObjectId: return value.AsObjectId.ToString();
                default:
                    // Fallback for types not explicitly handled
                    return value.ToString();
            }
        }

        /// <summary>
        /// Compute an MD5 hash from the record’s Data dictionary, sorted by key for deterministic ordering.
        /// </summary>
        private string ComputeHash(MongoDbRecord record)
        {
            var sb = new StringBuilder();
            foreach (var kv in record.Data.OrderBy(x => x.Key))
            {
                sb.Append(kv.Key).Append('=').Append(kv.Value ?? "null").Append(';');
            }

            using var md5 = MD5.Create();
            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            var hashBytes = md5.ComputeHash(bytes);
            return Convert.ToBase64String(hashBytes);
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
                if (ex != null)
                {
                    Console.WriteLine($"ERROR: {message}\n{ex}");
                }
                else
                {
                    Console.WriteLine($"ERROR: {message}");
                }
            }
        }
    }
}
