using Azure.Messaging.ServiceBus;
using Cortex.Streams.AzureServiceBus.Serializers;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Streams.AzureServiceBus
{
    /// <summary>
    /// Azure Service Bus Sink Operator with serialization support.
    /// </summary>
    /// <typeparam name="TInput">The type of objects to send.</typeparam>
    public class AzureServiceBusSinkOperator<TInput> : ISinkOperator<TInput>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _queueOrTopicName;
        private readonly ISerializer<TInput> _serializer;
        private ServiceBusClient _client;
        private ServiceBusSender _sender;
        private bool _isRunning;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSinkOperator{TInput}"/> class.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="queueOrTopicName">The name of the queue or topic to send messages to.</param>
        /// <param name="serializer">The serializer to convert TInput objects to strings.</param>
        public AzureServiceBusSinkOperator(string connectionString, string queueOrTopicName, ISerializer<TInput>? serializer = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _queueOrTopicName = queueOrTopicName ?? throw new ArgumentNullException(nameof(queueOrTopicName));

            _serializer = serializer ?? new DefaultJsonSerializer<TInput>();
        }

        /// <summary>
        /// Starts the sink operator.
        /// </summary>
        public void Start()
        {
            if (_isRunning) throw new InvalidOperationException("AzureServiceBusSinkOperator is already running.");

            _client = new ServiceBusClient(_connectionString);
            _sender = _client.CreateSender(_queueOrTopicName);

            _isRunning = true;
        }

        /// <summary>
        /// Processes the input object by serializing it and sending it to the specified Azure Service Bus queue or topic.
        /// </summary>
        /// <param name="input">The input object to send.</param>
        public void Process(TInput input)
        {
            if (!_isRunning)
            {
                Console.WriteLine("AzureServiceBusSinkOperator is not running. Call Start() before processing messages.");
                return;
            }

            if (input == null)
            {
                Console.WriteLine("AzureServiceBusSinkOperator received null input. Skipping.");
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
            Console.WriteLine("AzureServiceBusSinkOperator stopped.");
        }

        /// <summary>
        /// Sends a serialized message to Azure Service Bus asynchronously.
        /// </summary>
        /// <param name="obj">The input object to send.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task SendMessageAsync(TInput obj)
        {
            var serializedMessage = _serializer.Serialize(obj);
            var serviceBusMessage = new ServiceBusMessage(serializedMessage)
            {
                ContentType = "application/json",
                Subject = typeof(TInput).Name
            };

            try
            {
                await _sender.SendMessageAsync(serviceBusMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending message to Azure Service Bus: {ex.Message}");
                // TODO: Implement retry logic or send to a dead-letter queue as needed.
            }
        }

        /// <summary>
        /// Disposes the Azure Service Bus client and sender.
        /// </summary>
        public void Dispose()
        {
            _sender?.DisposeAsync().AsTask().Wait();
            _client?.DisposeAsync().AsTask().Wait();
        }
    }
}
