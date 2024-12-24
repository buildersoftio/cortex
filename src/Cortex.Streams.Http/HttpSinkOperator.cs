using Cortex.Streams.Operators;
using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cortex.Streams.Http
{
    /// <summary>
    /// A sink operator that pushes data to an HTTP endpoint.
    /// </summary>
    /// <typeparam name="TInput">The type of data consumed by the HTTP sink operator.</typeparam>
    public class HttpSinkOperator<TInput> : ISinkOperator<TInput>
    {
        private readonly string _endpoint;
        private readonly HttpClient _httpClient;
        private readonly JsonSerializerOptions _jsonOptions;

        // Retry configuration
        private readonly int _maxRetries;
        private readonly TimeSpan _initialDelay;

        /// <summary>
        /// Creates a new HttpSinkOperator.
        /// </summary>
        /// <param name="endpoint">The endpoint where data should be posted.</param>
        /// <param name="maxRetries">Number of max consecutive retries on failure before giving up.</param>
        /// <param name="initialDelay">Initial backoff delay when retrying.</param>
        /// <param name="httpClient">Optional HttpClient. If null, a new HttpClient will be created.</param>
        /// <param name="jsonOptions">Optional JsonSerializerOptions for serializing JSON.</param>
        public HttpSinkOperator(
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
        /// Called once when the sink operator is started.
        /// </summary>
        public void Start()
        {
            // Any initialization or connection setup if needed
        }

        /// <summary>
        /// Processes each incoming item from the stream, pushing it to the HTTP endpoint with retries.
        /// </summary>
        /// <param name="input">The data to send.</param>
        public void Process(TInput input)
        {
            // Synchronous approach
            // For a synchronous approach, we do blocking calls
            int attempt = 0;
            TimeSpan delay = _initialDelay;

            while (true)
            {
                try
                {
                    var json = JsonSerializer.Serialize(input, _jsonOptions);
                    var content = new StringContent(json, Encoding.UTF8, "application/json");

                    using var response = _httpClient.PostAsync(_endpoint, content).Result;
                    response.EnsureSuccessStatusCode();

                    // success
                    break;
                }
                catch (Exception ex)
                {
                    attempt++;
                    if (attempt > _maxRetries)
                    {
                        Console.WriteLine($"HttpSinkOperator: Exhausted retries for endpoint {_endpoint}. Error: {ex.Message}");
                        break;
                    }

                    Console.WriteLine($"HttpSinkOperator: Error sending data (attempt {attempt} of {_maxRetries}). Retrying in {delay}. Error: {ex.Message}");
                    Task.Delay(delay).Wait();

                    // Exponential backoff
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                }
            }
        }

        /// <summary>
        /// Called once when the sink operator is stopped.
        /// </summary>
        public void Stop()
        {
            // Cleanup if needed
        }
    }
}