using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Operators;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Streams.Elasticsearch
{
    /// <summary>
    /// A sink operator that writes data to an Elasticsearch index in bulk.
    /// 
    /// If some documents fail to index, they are stored in an IDataStore for later retries.
    /// A background service attempts to resend failed documents at a specified interval.
    /// </summary>
    /// <typeparam name="TInput">Type of the document to be indexed.</typeparam>
    public class ElasticsearchSinkOperator<TInput> : ISinkOperator<TInput>, IStatefulOperator
    {
        private readonly ElasticsearchClient _client;
        private readonly string _indexName;
        private readonly IDataStore<string, TInput> _failedDocumentsStore;
        private readonly ILogger<ElasticsearchSinkOperator<TInput>> _logger;

        // Local in-memory buffer for the current batch to be flushed.
        private readonly List<TInput> _currentBatch;
        private readonly object _batchLock = new object();

        // The maximum number of documents to collect before we issue a bulk request.
        private readonly int _batchSize;

        // The interval at which the background timer attempts to reprocess any failed documents in the store.
        private readonly TimeSpan _retryInterval;

        // Timer for retrying previously failed documents.
        private System.Timers.Timer _retryTimer;

        // Flush interval settings
        private readonly TimeSpan _flushInterval;
        private System.Timers.Timer _flushTimer;

        // Flag indicating operator is started (to handle concurrency safely).
        private volatile bool _isStarted;

        /// <summary>
        /// Creates a new Elasticsearch Sink Operator using the 8.x client.
        /// </summary>
        /// <param name="client">
        /// The configured ElasticsearchClient used to send data to Elasticsearch.
        /// </param>
        /// <param name="indexName">
        /// The name of the index to which documents should be written.
        /// </param>
        /// <param name="failedDocumentsStore">
        /// An IDataStore used to store any documents that fail to index. 
        /// They will be retried later automatically.
        /// </param>
        /// <param name="logger">
        /// An optional logger for information and error messages. If null, logs go to Console.
        /// </param>
        /// <param name="batchSize">
        /// Optional batch size. When the in-flight buffer hits this size, a bulk request is triggered.
        /// Defaults to 50.
        /// </param>
        /// <param name="retryInterval">
        /// How often we retry failed documents from the IDataStore.
        /// Defaults to 60 seconds.
        /// </param>
        public ElasticsearchSinkOperator(
            ElasticsearchClient client,
            string indexName,
            int batchSize = 50,
            TimeSpan? flushInterval = null,
            TimeSpan? retryInterval = null,
            IDataStore<string, TInput> failedDocumentsStore = null,
            ILogger<ElasticsearchSinkOperator<TInput>> logger = null)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));

            _failedDocumentsStore = failedDocumentsStore ?? new InMemoryStateStore<string, TInput>("default_failedDocuments");

            _logger = logger;

            _batchSize = batchSize;
            _retryInterval = retryInterval ?? TimeSpan.FromSeconds(60);

            // If flushInterval is not set or set to TimeSpan.Zero, the flush-timer won't be started.
            _flushInterval = flushInterval ?? TimeSpan.Zero;

            _currentBatch = new List<TInput>();
        }

        /// <summary>
        /// Called by the pipeline to process each record. 
        /// Accumulates into a batch, then flushes to ES once we exceed _batchSize.
        /// </summary>
        public void Process(TInput input)
        {
            if (!_isStarted)
            {
                LogError("Process called before the ElasticsearchSinkOperator was started. Please start the operator before start processing.");
            }

            lock (_batchLock)
            {
                _currentBatch.Add(input);

                if (_currentBatch.Count >= _batchSize)
                {
                    FlushBatch(_currentBatch);
                    _currentBatch.Clear();
                }
            }
        }

        /// <summary>
        /// Start the operator, initializing any resources or background tasks if needed.
        /// </summary>
        public void Start()
        {
            if (_isStarted) return;

            _isStarted = true;
            LogInformation($"ElasticsearchSinkOperator started. Will retry failed docs every {_retryInterval}.");

            // Create a timer to periodically retry failed documents in the store.
            _retryTimer = new System.Timers.Timer(_retryInterval.TotalMilliseconds);
            _retryTimer.Elapsed += (s, e) => RetryFailedDocuments();
            _retryTimer.AutoReset = true;
            _retryTimer.Start();

            // If flushInterval is set (> 0), set up a flush timer as well.
            if (_flushInterval > TimeSpan.Zero)
            {
                LogInformation($"Flush interval set to {_flushInterval}. Will flush partial batches on this schedule.");
                _flushTimer = new System.Timers.Timer(_flushInterval.TotalMilliseconds);
                _flushTimer.Elapsed += (s, e) => FlushPendingBatch();
                _flushTimer.AutoReset = true;
                _flushTimer.Start();
            }
        }

        /// <summary>
        /// Stop the operator, flushing any in-memory data and disposing of resources.
        /// </summary>
        public void Stop()
        {
            if (!_isStarted) return;

            _isStarted = false;
            LogInformation("ElasticsearchSinkOperator stopping.");

            // Stop the background timers and dispose them.
            _retryTimer?.Stop();
            _retryTimer?.Dispose();

            _flushTimer?.Stop();
            _flushTimer?.Dispose();

            // Flush any remaining documents in the current batch.
            FlushPendingBatch();

            LogInformation("ElasticsearchSinkOperator stopped.");
        }

        /// <summary>
        /// Flush the current batch if there are any items present.
        /// </summary>
        private void FlushPendingBatch()
        {
            lock (_batchLock)
            {
                if (_currentBatch.Count > 0)
                {
                    LogInformation($"FlushPendingBatch triggered. Current batch size: {_currentBatch.Count}.");
                    FlushBatch(_currentBatch);
                    _currentBatch.Clear();
                }
            }
        }

        /// <summary>
        /// Flush a batch of documents to Elasticsearch in bulk.
        /// Any failed items are persisted into the IDataStore for later retry.
        /// </summary>
        private void FlushBatch(IList<TInput> batch)
        {
            if (batch.Count == 0) return;

            LogInformation($"Flushing batch of size {batch.Count} to Elasticsearch index '{_indexName}'.");

            // Build a BulkRequest with index operations:
            var bulkRequest = new BulkRequest(_indexName)
            {
                Operations = new List<IBulkOperation>()
            };

            foreach (var doc in batch)
            {
                bulkRequest.Operations.Add(new BulkIndexOperation<TInput>(doc));
            }

            var response = _client.BulkAsync(bulkRequest).Result;

            // Check top-level success
            if (!response.IsSuccess())
            {
                var errorMsg = $"Bulk request failed entirely: {response.DebugInformation}";
                LogError(errorMsg);

                // Store everything in the retry store.
                foreach (var doc in batch)
                {
                    _failedDocumentsStore.Put(Guid.NewGuid().ToString(), doc);
                }
                return;
            }

            // If top-level succeeded, check for partial failures
            if (response.Errors)
            {
                foreach (var item in response.Items)
                {
                    if (item.Status >= 300) // e.g., 400, 404, 500...
                    {
                        var reason = item.Error?.Reason ?? "Unknown reason";
                        LogError($"Partial failure for item. Status={item.Status}, Reason={reason}");

                        // For robust correlation, track item.Id or external ID mapping.
                        var failedDoc = batch.FirstOrDefault();
                        _failedDocumentsStore.Put(Guid.NewGuid().ToString(), failedDoc);
                    }
                }
            }
        }

        /// <summary>
        /// Attempts to reindex documents that previously failed.
        /// </summary>
        private void RetryFailedDocuments()
        {
            var allFailed = _failedDocumentsStore.GetAll().ToList();
            if (!allFailed.Any()) return;

            LogInformation($"Retrying {allFailed.Count} failed documents...");

            var bulkRequest = new BulkRequest(_indexName)
            {
                Operations = new List<IBulkOperation>()
            };

            // Correlate each operation with its store key.
            // This lets us remove only those that succeed.
            foreach (var (storeKey, doc) in allFailed)
            {
                var indexOperation = new BulkIndexOperation<TInput>(doc)
                {
                    Id = storeKey // correlation ID for partial success/failure checks
                };
                bulkRequest.Operations.Add(indexOperation);
            }

            var response = _client.BulkAsync(bulkRequest).Result;

            if (!response.IsSuccess())
            {
                var errorMsg = $"Bulk retry request failed: {response.DebugInformation}";
                LogError(errorMsg);
                return;
            }

            // If bulk is overall successful, we still check items for partial failures.
            if (response.Errors)
            {
                foreach (var item in response.Items)
                {
                    if (item.Status < 300)
                    {
                        // Successfully reindexed; remove from store.
                        _failedDocumentsStore.Remove(item.Id);
                    }
                    else
                    {
                        // Partial failure; doc remains in store for next retry.
                        LogError($"Retry partial failure: Status={item.Status}, Reason={item.Error?.Reason}");
                    }
                }
            }
            else
            {
                // If there were no errors at all, remove every doc we retried.
                foreach (var (storeKey, _) in allFailed)
                {
                    _failedDocumentsStore.Remove(storeKey);
                }
            }
        }

        // --------------------------------------------------------------------
        // LOGGING HELPERS
        // --------------------------------------------------------------------
        private void LogInformation(string message)
        {
            if (_logger != null)
            {
                _logger.LogInformation(message);
            }
            else
            {
                Console.WriteLine(message);
            }
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

        /// <summary>
        /// Implementation of IStatefulOperator for exposing the store(s).
        /// </summary>
        public IEnumerable<IDataStore> GetStateStores()
        {
            // Return any data stores that we rely on for statefulness.
            return new[] { _failedDocumentsStore };
        }
    }
}
