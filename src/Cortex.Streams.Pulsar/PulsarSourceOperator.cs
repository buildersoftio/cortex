using Cortex.Streams.Abstractions;
using DotPulsar.Abstractions;
using DotPulsar;
using DotPulsar.Extensions;
using Cortex.Streams.Pulsar.Deserializers;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using System;

namespace Cortex.Streams.Pulsar
{
    public class PulsarSourceOperator<TOutput> : ISourceOperator<TOutput>
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

        public void Start(Action<TOutput> emit)
        {
            _cts = new CancellationTokenSource();

            if (_consumerOptions == null)
            {
                _consumer = _client.NewConsumer()
                    .Topic(_topic)
                    .InitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscriptionName($"subscription-{Guid.NewGuid()}")
                    .Create();
            }
            else
            {
                _consumer = _client
                    .CreateConsumer(_consumerOptions);
            }


            _consumeTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var message in _consumer.Messages(_cts.Token))
                    {
                        var data = message.Data;
                        var output = _deserializer.Deserialize(data);
                        emit(output);
                        await _consumer.Acknowledge(message, _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Consume loop canceled
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
