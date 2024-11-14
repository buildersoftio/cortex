using Azure.Storage.Blobs;
using Cortex.Streams.AzureBlobStorage.Serializers;
using Cortex.Streams.Operators;
using Polly;
using Polly.Retry;

namespace Cortex.Streams.AzureBlobStorage
{
    public class AzureBlobStorageBulkSinkOperator<TInput> : ISinkOperator<TInput>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _containerName;
        private readonly string _directoryPath;
        private readonly ISerializer<TInput> _serializer;
        private readonly BlobContainerClient _containerClient;
        private bool _isRunning;

        // For batching
        private readonly List<TInput> _buffer = new List<TInput>();
        private readonly int _batchSize;
        private readonly TimeSpan _flushInterval;
        private readonly Timer _timer;
        private readonly AsyncRetryPolicy _retryPolicy;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobStorageSinkOperator{TInput}"/> class.
        /// </summary>
        /// <param name="connectionString">Azure Blob Storage connection string.</param>
        /// <param name="containerName">Name of the Blob container.</param>
        /// <param name="directoryPath">Path within the container to store data (e.g., "data/ingest").</param>
        /// <param name="serializer">Serializer to convert TInput objects to strings.</param>
        /// <param name="batchSize">Number of messages to batch before uploading.</param>
        /// <param name="flushInterval">Time interval to flush the buffer regardless of batch size.</param>
        public AzureBlobStorageBulkSinkOperator(
            string connectionString,
            string containerName,
            string directoryPath,
            ISerializer<TInput> serializer,
            int batchSize = 100,
            TimeSpan? flushInterval = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _containerName = containerName ?? throw new ArgumentNullException(nameof(containerName));
            _directoryPath = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _batchSize = batchSize;
            _flushInterval = flushInterval ?? TimeSpan.FromSeconds(10);

            _containerClient = new BlobContainerClient(_connectionString, _containerName);
            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        Console.WriteLine($"Retry {retryCount} after {timeSpan} due to {exception.Message}");
                    });

            _timer = new Timer(async _ => await FlushBufferAsync(), null, _flushInterval, _flushInterval);
        }

        /// <summary>
        /// Starts the sink operator by ensuring the Blob container exists.
        /// </summary>
        public void Start()
        {
            if (_isRunning) throw new InvalidOperationException("AzureBlobStorageSinkOperator is already running.");

            _containerClient.CreateIfNotExists();
            _isRunning = true;
        }

        /// <summary>
        /// Processes the input object by adding it to the buffer.
        /// </summary>
        /// <param name="input">The input object to send.</param>
        public void Process(TInput input)
        {
            if (!_isRunning)
            {
                Console.WriteLine("AzureBlobStorageSinkOperator is not running. Call Start() before processing messages.");
                return;
            }

            if (input == null)
            {
                Console.WriteLine("AzureBlobStorageSinkOperator received null input. Skipping.");
                return;
            }

            lock (_buffer)
            {
                _buffer.Add(input);
                if (_buffer.Count >= _batchSize)
                {
                    var batch = new List<TInput>(_buffer);
                    _buffer.Clear();
                    Task.Run(() => SendBatchAsync(batch));
                }
            }
        }

        /// <summary>
        /// Stops the sink operator by flushing the buffer and disposing resources.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;

            _timer.Dispose();
            FlushBufferAsync().Wait();
            Dispose();
            _isRunning = false;
            Console.WriteLine("AzureBlobStorageSinkOperator stopped.");
        }

        /// <summary>
        /// Sends a batch of serialized messages to Azure Blob Storage asynchronously.
        /// </summary>
        /// <param name="batch">The batch of input objects to send.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task SendBatchAsync(List<TInput> batch)
        {
            var serializedBatch = string.Join(Environment.NewLine, batch.Select(obj => _serializer.Serialize(obj)));
            var fileName = $"{Guid.NewGuid()}.jsonl"; // JSON Lines format
            var blobName = $"{_directoryPath}/{fileName}";
            var blobClient = _containerClient.GetBlobClient(blobName);

            await _retryPolicy.ExecuteAsync(async () =>
            {
                using var stream = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(serializedBatch));
                await blobClient.UploadAsync(stream, new Azure.Storage.Blobs.Models.BlobHttpHeaders { ContentType = "application/jsonl" });
            });
        }

        /// <summary>
        /// Flushes the buffer by sending any remaining messages as a batch.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task FlushBufferAsync()
        {
            List<TInput> batch = null;
            lock (_buffer)
            {
                if (_buffer.Count > 0)
                {
                    batch = new List<TInput>(_buffer);
                    _buffer.Clear();
                }
            }

            if (batch != null && batch.Count > 0)
            {
                await SendBatchAsync(batch);
            }
        }

        /// <summary>
        /// Disposes the Blob container client and timer.
        /// </summary>
        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
