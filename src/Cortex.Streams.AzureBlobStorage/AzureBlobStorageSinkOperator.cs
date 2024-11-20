using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs;
using Polly.Retry;
using Polly;
using Cortex.Streams.AzureBlobStorage.Serializers;
using Cortex.Streams.Operators;
using System;
using System.Threading.Tasks;

namespace Cortex.Streams.AzureBlobStorage
{
    /// <summary>
    /// Azure Blob Storage Sink Operator that writes each serialized data object as a separate blob file.
    /// </summary>
    /// <typeparam name="TInput">The type of objects to send.</typeparam>
    public class AzureBlobStorageSinkOperator<TInput> : ISinkOperator<TInput>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _containerName;
        private readonly string _directoryPath;
        private readonly ISerializer<TInput> _serializer;
        private readonly BlobContainerClient _containerClient;
        private bool _isRunning;

        // Retry policy using Polly
        private readonly AsyncRetryPolicy _retryPolicy;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobStorageSinkOperator{TInput}"/> class.
        /// </summary>
        /// <param name="connectionString">Azure Blob Storage connection string.</param>
        /// <param name="containerName">Name of the Blob container.</param>
        /// <param name="directoryPath">Path within the container to store data (e.g., "data/ingest").</param>
        /// <param name="serializer">Serializer to convert TInput objects to strings.</param>
        public AzureBlobStorageSinkOperator(
            string connectionString,
            string containerName,
            string directoryPath,
            ISerializer<TInput>? serializer = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _containerName = containerName ?? throw new ArgumentNullException(nameof(containerName));
            _directoryPath = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));
            _serializer = serializer ?? new DefaultJsonSerializer<TInput>();

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
        }

        /// <summary>
        /// Starts the sink operator by ensuring the Blob container exists.
        /// </summary>
        public void Start()
        {
            if (_isRunning) throw new InvalidOperationException("AzureBlobStorageSinkOperator is already running.");

            _containerClient.CreateIfNotExists(PublicAccessType.None);
            _isRunning = true;
            Console.WriteLine($"AzureBlobStorageSinkOperator started and connected to container '{_containerName}', directory '{_directoryPath}'.");
        }

        /// <summary>
        /// Processes the input object by serializing it and sending it as a separate blob file.
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

            Task.Run(() => SendMessageAsync(input));
        }

        /// <summary>
        /// Sends a serialized message to Azure Blob Storage asynchronously as a separate blob.
        /// </summary>
        /// <param name="obj">The input object to send.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task SendMessageAsync(TInput obj)
        {
            var serializedMessage = _serializer.Serialize(obj);
            var blobName = $"{_directoryPath}/{Guid.NewGuid()}.json"; // e.g., data/ingest/unique-id.json
            var blobClient = _containerClient.GetBlobClient(blobName);

            try
            {
                using var stream = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(serializedMessage));
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    await blobClient.UploadAsync(stream, new BlobHttpHeaders { ContentType = "application/json" });
                    Console.WriteLine($"Message uploaded to Azure Blob Storage: {blobName}");
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading message to Azure Blob Storage: {ex.Message}");
                // TODO: Implement dead-lettering or alternative handling as needed.
            }
        }

        /// <summary>
        /// Stops the sink operator by disposing resources.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;

            Dispose();
            _isRunning = false;
            Console.WriteLine("AzureBlobStorageSinkOperator stopped.");
        }

        /// <summary>
        /// Disposes the Blob container client.
        /// </summary>
        public void Dispose()
        {

        }
    }
}
