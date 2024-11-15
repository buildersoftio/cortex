using Azure.Messaging.ServiceBus;
using Cortex.Streams.AzureServiceBus.Deserializers;
using Cortex.Streams.Operators;
using System;
using System.Threading.Tasks;

namespace Cortex.Streams.AzureServiceBus
{
    /// <summary>
    /// Azure Service Bus Source Operator with deserialization support.
    /// </summary>
    /// <typeparam name="TOutput">The type of objects to emit.</typeparam>
    public class AzureServiceBusSourceOperator<TOutput> : ISourceOperator<TOutput>, IDisposable
    {
        private readonly string _connectionString;
        private readonly string _queueOrTopicName;
        private readonly IDeserializer<TOutput> _deserializer;
        private readonly ServiceBusProcessorOptions _serviceBusProcessorOptions;
        private ServiceBusProcessor _processor;
        private Action<TOutput> _emitAction;
        private bool _isRunning;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSourceOperator{TOutput}"/> class.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="queueOrTopicName">The name of the queue or topic to consume from.</param>
        /// <param name="deserializer">The deserializer to convert message strings to TOutput objects, default is DefaultJsonDeserializer</param>
        public AzureServiceBusSourceOperator(string connectionString, string queueOrTopicName, IDeserializer<TOutput>? deserializer = null, ServiceBusProcessorOptions serviceBusProcessorOptions = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _queueOrTopicName = queueOrTopicName ?? throw new ArgumentNullException(nameof(queueOrTopicName));

            _deserializer = deserializer ?? new DefaultJsonDeserializer<TOutput>();

            _serviceBusProcessorOptions = serviceBusProcessorOptions ?? new ServiceBusProcessorOptions()
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            };

        }

        /// <summary>
        /// Starts the source operator.
        /// </summary>
        /// <param name="emit">The action to emit deserialized objects into the stream.</param>
        public void Start(Action<TOutput> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));
            if (_isRunning) throw new InvalidOperationException("AzureServiceBusSourceOperator is already running.");

            _emitAction = emit;

            var client = new ServiceBusClient(_connectionString);
            _processor = client.CreateProcessor(_queueOrTopicName, _serviceBusProcessorOptions);

            _processor.ProcessMessageAsync += MessageHandler;
            _processor.ProcessErrorAsync += ErrorHandler;

            Task.Run(async () => await _processor.StartProcessingAsync());

            _isRunning = true;
        }

        /// <summary>
        /// Stops the source operator.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;

            Task.Run(async () => await _processor.StopProcessingAsync()).Wait();
            Dispose();
            _isRunning = false;
            Console.WriteLine("AzureServiceBusSourceOperator stopped.");
        }

        /// <summary>
        /// Handles incoming messages.
        /// </summary>
        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            try
            {
                var body = args.Message.Body.ToString();
                var obj = _deserializer.Deserialize(body);
                _emitAction?.Invoke(obj);

                // Complete the message. Messages is deleted from the queue. 
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                // Optionally abandon the message or dead-letter it
                await args.AbandonMessageAsync(args.Message);
            }
        }

        /// <summary>
        /// Handles errors during message processing.
        /// </summary>
        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Error in AzureServiceBusSourceOperator: {args.Exception.Message}");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disposes the ServiceBusProcessor.
        /// </summary>
        public void Dispose()
        {
            _processor?.DisposeAsync().AsTask().Wait();
        }
    }
}
