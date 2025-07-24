using Cortex.Streams.Operators;
using DotPulsar.Abstractions;
using DotPulsar;
using Cortex.Streams.Pulsar.Serializers;
using DotPulsar.Extensions;
using System.Buffers;
using System;

namespace Cortex.Streams.Pulsar
{
    public class PulsarSinkOperator<TInput> : ISinkOperator<TInput>
    {
        private readonly string _serviceUrl;
        private readonly string _topic;
        private readonly ISerializer<TInput> _serializer;
        private readonly IPulsarClient _client;
        private IProducer<ReadOnlySequence<byte>> _producer;

        public PulsarSinkOperator(string serviceUrl, string topic, ISerializer<TInput> serializer = null)
        {
            _serviceUrl = serviceUrl;
            _topic = topic;
            _serializer = serializer;

            _serializer ??= new DefaultJsonSerializer<TInput>();


            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(_serviceUrl))
                .Build();

            // BUG #103 Start PulsarSink Operator when Sink is initialized
            // Pulsar Producer doesnot start when the production happens, we have to start the Producer when it is initialized.
            Start();
        }

        public void Start()
        {
            _producer = _client.NewProducer()
                .Topic(_topic)
                .Create();
        }

        public void Process(TInput input)
        {
            var data = _serializer.Serialize(input);
            _producer.Send(data);
        }

        public void Stop()
        {
            _producer.DisposeAsync().AsTask().Wait();
            _client.DisposeAsync().AsTask().Wait();
        }
    }
}
