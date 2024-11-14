using Amazon.S3;
using Amazon.S3.Transfer;
using Cortex.Streams.Operators;
using Cortex.Streams.S3.Serializers;
using System;
using System.Threading.Tasks;

namespace Cortex.Streams.S3
{
    /// <summary>
    /// AWS S3 Sink Operator that writes serialized data to an S3 bucket.
    /// </summary>
    /// <typeparam name="TInput">The type of objects to send.</typeparam>
    public class S3SinkOperator<TInput> : ISinkOperator<TInput>, IDisposable
    {
        private readonly string _bucketName;
        private readonly string _folderPath;
        private readonly ISerializer<TInput> _serializer;
        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtility _transferUtility;
        private bool _isRunning;

        /// <summary>
        /// Initializes a new instance of the <see cref="S3SinkOperator{TInput}"/> class.
        /// </summary>
        /// <param name="bucketName">Name of the S3 bucket.</param>
        /// <param name="folderPath">Path within the bucket to store data (e.g., "data/ingest").</param>
        /// <param name="s3Client">Instance of IAmazonS3 for interacting with AWS S3.</param>
        /// <param name="serializer">Serializer to convert TInput objects to strings. Default is DefaultJsonSerializer</param>
        public S3SinkOperator(string bucketName, string folderPath, IAmazonS3 s3Client, ISerializer<TInput>? serializer = null)
        {
            _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
            _folderPath = folderPath ?? throw new ArgumentNullException(nameof(folderPath));

            _serializer = serializer ?? new DefaultJsonSerializer<TInput>();

            _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
            _transferUtility = new TransferUtility(_s3Client);
        }

        /// <summary>
        /// Starts the sink operator.
        /// </summary>
        public void Start()
        {
            if (_isRunning) throw new InvalidOperationException("S3SinkOperator is already running.");

            _isRunning = true;
        }

        /// <summary>
        /// Processes the input object by serializing it and sending it to AWS S3.
        /// </summary>
        /// <param name="input">The input object to send.</param>
        public void Process(TInput input)
        {
            if (!_isRunning)
            {
                Console.WriteLine("S3SinkOperator is not running. Call Start() before processing messages.");
                return;
            }

            if (input == null)
            {
                Console.WriteLine("S3SinkOperator received null input. Skipping.");
                return;
            }

            Task.Run(() => SendMessageAsync(input));
        }

        /// <summary>
        /// Stops the sink operator.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;

            Dispose();
            _isRunning = false;
            Console.WriteLine("S3SinkOperator stopped.");
        }

        /// <summary>
        /// Sends a serialized message to AWS S3 asynchronously.
        /// </summary>
        /// <param name="obj">The input object to send.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task SendMessageAsync(TInput obj)
        {
            var serializedMessage = _serializer.Serialize(obj);
            var fileName = $"{Guid.NewGuid()}.json"; // e.g., unique-id.json
            var key = $"{_folderPath}/{fileName}";

            try
            {
                using System.IO.MemoryStream stream = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(serializedMessage));
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    Key = key,
                    BucketName = _bucketName,
                    ContentType = "application/json"
                };

                await _transferUtility.UploadAsync(uploadRequest);
            }
            catch (AmazonS3Exception s3Ex)
            {
                Console.WriteLine($"Error uploading message to S3: {s3Ex.Message}");
                // TODO: Implement retry logic or send to a dead-letter location as needed.
            }
            catch (Exception ex)
            {
                Console.WriteLine($"General error uploading message to S3: {ex.Message}");
                // TODO: Implement additional error handling as needed.
            }
        }

        /// <summary>
        /// Disposes the AWS S3 client and transfer utility.
        /// </summary>
        public void Dispose()
        {
            _transferUtility?.Dispose();
            _s3Client?.Dispose();
        }
    }
}
