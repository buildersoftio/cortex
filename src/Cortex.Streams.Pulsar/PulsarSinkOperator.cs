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

        private readonly Func<TInput, string> _keySelector; // optional

        public PulsarSinkOperator(string serviceUrl,
            string topic,
            Func<TInput, string> keySelector = null,
            ISerializer<TInput> serializer = null)
        {
            _serviceUrl = serviceUrl;
            _topic = topic;
            _serializer = serializer;

            _serializer ??= new DefaultJsonSerializer<TInput>();

            _keySelector = keySelector;

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
            if (_keySelector is null)
            {
                _producer.Send(data); // current behavior :contentReference[oaicite:17]{index=17}
            }
            else
            {
                var metadata = new MessageMetadata { Key = _keySelector(input) };
                _producer.Send(metadata, new ReadOnlySequence<byte>(data));
            }
        }

        public void Stop()
        {
            _producer.DisposeAsync().AsTask().Wait();
            _client.DisposeAsync().AsTask().Wait();
        }
    }
}
