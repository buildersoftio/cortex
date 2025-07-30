using Confluent.Kafka;
using Cortex.Streams.Kafka.Deserializers;
using Cortex.Streams.Operators;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Kafka
{
    /// <summary>
    /// Kafka source that emits KeyValuePair<TKey, TValue> so the pipeline can use message keys.
    /// </summary>
    public sealed class KafkaSourceOperator<TKey, TValue> : ISourceOperator<KeyValuePair<TKey, TValue>>
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly IConsumer<TKey, TValue> _consumer;
        private CancellationTokenSource _cts;
        private Task _consumeTask;


        public KafkaSourceOperator(string bootstrapServers,
            string topic,
            ConsumerConfig config = null,
            IDeserializer<TKey> keyDeserializer = null,
            IDeserializer<TValue> valueDeserializer = null)
        {
            _bootstrapServers = bootstrapServers ?? throw new ArgumentNullException(nameof(bootstrapServers));
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));

            var consumerConfig = config ?? new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
            };

            keyDeserializer ??= new DefaultJsonDeserializer<TKey>();
            valueDeserializer ??= new DefaultJsonDeserializer<TValue>();

            _consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .Build();
        }


        public void Start(Action<KeyValuePair<TKey, TValue>> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));

            _cts = new CancellationTokenSource();
            _consumer.Subscribe(_topic);

            _consumeTask = Task.Run(() =>
            {
                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        var result = _consumer.Consume(_cts.Token);
                        emit(new KeyValuePair<TKey, TValue>(result.Message.Key, result.Message.Value));
                    }
                }
                catch (OperationCanceledException)
                {
                    // shutting down - consume loop canceled
                }
                finally
                {
                    _consumer.Close();
                }
            }, _cts.Token);
        }

        public void Stop()
        {
            if (_cts == null)
                return;

            _cts.Cancel();
            try
            {
                _consumeTask?.Wait();
            }
            catch
            {
                /* swallow aggregate canceled */
            }

            _consumer.Dispose();
            _cts.Dispose();
        }
    }
}
