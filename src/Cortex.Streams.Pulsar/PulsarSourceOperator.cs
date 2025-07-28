using Cortex.Streams.Operators;
using Cortex.Streams.Pulsar.Deserializers;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Pulsar
{
    public class PulsarSourceOperator<TOutput> : ISourceOperator<KeyValuePair<string, TOutput>>
    {
        private readonly string _serviceUrl;
        private readonly ConsumerOptions<ReadOnlySequence<byte>> _consumerOptions;
        private readonly string _topic;
        private readonly IDeserializer<TOutput> _deserializer;
        private readonly IPulsarClient _client;
        private IConsumer<ReadOnlySequence<byte>> _consumer;
        private CancellationTokenSource _cts;
        private Task _consumeTask;

        public PulsarSourceOperator(string serviceUrl, string topic, IDeserializer<TOutput> deserializer = null)
        {
            _serviceUrl = serviceUrl;
            _topic = topic;
            _deserializer = deserializer;

            if (_deserializer is null)
                _deserializer = new DefaultJsonDeserializer<TOutput>();

            _consumerOptions = null;

            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(_serviceUrl))
                .Build();
        }

        public PulsarSourceOperator(string serviceUrl, ConsumerOptions<ReadOnlySequence<byte>> consumerOptions, IDeserializer<TOutput> deserializer = null)
        {
            _serviceUrl = serviceUrl;
            _consumerOptions = consumerOptions;
            _deserializer = deserializer;

            if (_deserializer is null)
                _deserializer = new DefaultJsonDeserializer<TOutput>();

            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(_serviceUrl))
                .Build();
        }

        public void Start(Action<KeyValuePair<string, TOutput>> emit)
        {
            _cts = new CancellationTokenSource();

            _consumer = _consumerOptions == null
                ? _client.NewConsumer()
                    .Topic(_topic)
                    .InitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscriptionName($"subscription-{Guid.NewGuid()}")
                    .Create()
                : _client.CreateConsumer(_consumerOptions);

            _consumeTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var message in _consumer.Messages(_cts.Token))
                    {
                        var key = message.Key ?? string.Empty; // Handle null keys gracefully
                        var output = _deserializer.Deserialize(message.Data);
                        emit(new KeyValuePair<string, TOutput>(key, output));
                        await _consumer.Acknowledge(message, _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested
                }
                finally
                {
                    await _consumer.DisposeAsync();
                    await _client.DisposeAsync();
                }
            }, _cts.Token);
        }

        public void Stop()
        {
            _cts.Cancel();
            _consumeTask.Wait();
        }
    }
}
