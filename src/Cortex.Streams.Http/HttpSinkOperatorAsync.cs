using Cortex.Streams.Operators;
using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Http
{
    /// <summary>
    /// A sink operator that pushes data to an HTTP endpoint asynchronously.
    /// </summary>
    /// <typeparam name="TInput">Type of data consumed by this sink.</typeparam>
    public class HttpSinkOperatorAsync<TInput> : ISinkOperator<TInput>
    {
        private readonly string _endpoint;
        private readonly HttpClient _httpClient;
        private readonly JsonSerializerOptions _jsonOptions;

        // Retry configuration
        private readonly int _maxRetries;
        private readonly TimeSpan _initialDelay;

        // Internal queue and background worker
        private BlockingCollection<TInput> _messageQueue;
        private CancellationTokenSource _cts;
        private Task _workerTask;

        /// <summary>
        /// Constructs an asynchronous HTTP sink operator.
        /// </summary>
        /// <param name="endpoint">The HTTP endpoint to which data should be posted.</param>
        /// <param name="maxRetries">Max consecutive retries on failure before giving up.</param>
        /// <param name="initialDelay">Initial backoff delay for retries.</param>
        /// <param name="httpClient">
        /// Optional <see cref="HttpClient"/>.  
        /// If null, a new HttpClient will be created (but consider <see cref="IHttpClientFactory"/> in production).
        /// </param>
        /// <param name="jsonOptions">Optional JSON serialization options.</param>
        public HttpSinkOperatorAsync(
            string endpoint,
            int maxRetries = 3,
            TimeSpan? initialDelay = null,
            HttpClient httpClient = null,
            JsonSerializerOptions jsonOptions = null)
        {
            _endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
            _maxRetries = maxRetries;
            _initialDelay = initialDelay ?? TimeSpan.FromMilliseconds(500);

            _httpClient = httpClient ?? new HttpClient();
            _jsonOptions = jsonOptions ?? new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        }

        /// <summary>
        /// Called once when the sink operator starts. Spawns a background worker.
        /// </summary>
        public void Start()
        {
            // Prepare the queue and cancellation token
            _messageQueue = new BlockingCollection<TInput>(boundedCapacity: 10000);
            _cts = new CancellationTokenSource();

            // Launch the worker that processes messages asynchronously
            _workerTask = Task.Run(() => WorkerLoopAsync(_cts.Token));
        }

        /// <summary>
        /// Queues incoming data for asynchronous sending.
        /// </summary>
        /// <param name="input">The data to be sent to the HTTP endpoint.</param>
        public void Process(TInput input)
        {
            // Enqueue the item. If the queue is full (bounded), this will block briefly.
            // If you want a non-blocking approach, consider _messageQueue.TryAdd(...).
            _messageQueue.Add(input);
        }

        /// <summary>
        /// Called once when the sink operator stops. Waits for the background worker to finish.
        /// </summary>
        public void Stop()
        {
            _cts.Cancel();
            _messageQueue.CompleteAdding();

            // Safely wait for the worker task to exit
            try
            {
                _workerTask.Wait();
            }
            catch (AggregateException ex)
            {
                // If the worker loop was canceled or faulted, handle if needed
                Console.WriteLine($"HttpSinkOperatorAsync: Worker stopped with exception: {ex.Message}");
            }

            _cts.Dispose();
            _messageQueue.Dispose();
        }

        /// <summary>
        /// Continuously consumes messages from the queue and sends them via HTTP asynchronously.
        /// </summary>
        private async Task WorkerLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested && !_messageQueue.IsCompleted)
            {
                TInput item;
                try
                {
                    // Blocks until an item is available or cancellation is requested
                    item = _messageQueue.Take(token);
                }
                catch (OperationCanceledException)
                {
                    // Gracefully exit when canceled
                    break;
                }
                catch (InvalidOperationException)
                {
                    // The collection has been marked as CompleteAdding
                    break;
                }

                // Send the item asynchronously (with retries)
                await SendAsync(item, token);
            }
        }

        /// <summary>
        /// Sends one item to the configured HTTP endpoint using exponential backoff.
        /// </summary>
        private async Task SendAsync(TInput item, CancellationToken token)
        {
            int attempt = 0;
            TimeSpan delay = _initialDelay;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    var json = JsonSerializer.Serialize(item, _jsonOptions);
                    var content = new StringContent(json, Encoding.UTF8, "application/json");

                    using var response = await _httpClient.PostAsync(_endpoint, content, token);
                    response.EnsureSuccessStatusCode();

                    // Success; break out of the loop
                    break;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    attempt++;
                    if (attempt > _maxRetries)
                    {
                        Console.WriteLine($"HttpSinkOperatorAsync: Exhausted retries for {_endpoint}. Error: {ex.Message}");
                        break;
                    }

                    Console.WriteLine($"HttpSinkOperatorAsync: Error sending data (attempt {attempt} of {_maxRetries}). " +
                                      $"Retrying in {delay}. Error: {ex.Message}");

                    // Exponential backoff, but only if not canceled
                    if (!token.IsCancellationRequested)
                    {
                        await Task.Delay(delay, token);
                        delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                    }
                }
            }
        }
    }
}
