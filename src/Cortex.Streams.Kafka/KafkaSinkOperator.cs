using Confluent.Kafka;
using Cortex.Streams.Kafka.Serializers;
using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Kafka
{
    public sealed class KafkaSinkOperator<TInput> : ISinkOperator<TInput>
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly IProducer<Null, TInput> _producer;

        public KafkaSinkOperator(string bootstrapServers, string topic, ProducerConfig config = null, ISerializer<TInput> serializer = null)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;

            var producerConfig = config ?? new ProducerConfig
            {
                BootstrapServers = _bootstrapServers
            };

            if (serializer == null)
                serializer = new DefaultJsonSerializer<TInput>();

            _producer = new ProducerBuilder<Null, TInput>(producerConfig)
                .SetValueSerializer(serializer)
                .Build();
        }

        public void Process(TInput input)
        {
            _producer.Produce(_topic, new Message<Null, TInput> { Value = input }, deliveryReport =>
            {
                if (deliveryReport.Error.IsError)
                {
                    Console.WriteLine($"Delivery Error: {deliveryReport.Error.Reason}");
                }
            });
        }

        public void Start()
        {
            // Any initialization if necessary
        }

        public void Stop()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
