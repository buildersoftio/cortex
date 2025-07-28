using Confluent.Kafka;
using Cortex.Streams.Kafka.Serializers;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;

namespace Cortex.Streams.Kafka
{
    /// <summary>
    /// Kafka sink that accepts KeyValuePair<TKey, TValue> so message keys are produced.
    /// </summary>
    public sealed class KafkaSinkOperator<TKey, TValue> : ISinkOperator<KeyValuePair<TKey, TValue>>
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly IProducer<TKey, TValue> _producer;

        public KafkaSinkOperator(
                   string bootstrapServers,
                   string topic,
                   ProducerConfig config = null,
                   ISerializer<TKey> keySerializer = null,
                   ISerializer<TValue> valueSerializer = null)
        {
            _bootstrapServers = bootstrapServers ?? throw new ArgumentNullException(nameof(bootstrapServers));
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));

            var producerConfig = config ?? new ProducerConfig
            {
                BootstrapServers = _bootstrapServers
            };

            keySerializer ??= new DefaultJsonSerializer<TKey>();
            valueSerializer ??= new DefaultJsonSerializer<TValue>();

            _producer = new ProducerBuilder<TKey, TValue>(producerConfig)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .Build();
        }

        public void Process(KeyValuePair<TKey, TValue> input)
        {
            var msg = new Message<TKey, TValue> { Key = input.Key, Value = input.Value };
            _producer.Produce(_topic, msg, deliveryReport =>
            {
                if (deliveryReport.Error.IsError)
                {
                    Console.WriteLine($"Delivery Error: {deliveryReport.Error.Reason}");
                }
            });
        }

        public void Start()
        {
            // no-op
        }

        public void Stop()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
