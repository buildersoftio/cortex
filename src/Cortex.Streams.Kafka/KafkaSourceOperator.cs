using Confluent.Kafka;
using Cortex.Streams.Kafka.Deserializers;
using Cortex.Streams.Operators;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Kafka
{
    public class KafkaSourceOperator<TOutput> : ISourceOperator<TOutput>
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly IConsumer<Ignore, TOutput> _consumer;
        private CancellationTokenSource _cts;
        private Task _consumeTask;

        public KafkaSourceOperator(string bootstrapServers, string topic, ConsumerConfig config = null, IDeserializer<TOutput> deserializer = null)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;

            var consumerConfig = config ?? new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
            };

            if (deserializer == null)
                deserializer = new DefaultJsonDeserializer<TOutput>();

            _consumer = new ConsumerBuilder<Ignore, TOutput>(consumerConfig)
                .SetValueDeserializer(deserializer)
                .Build();
        }

        public void Start(Action<TOutput> emit)
        {
            _cts = new CancellationTokenSource();
            _consumer.Subscribe(_topic);

            _consumeTask = Task.Run(() =>
            {
                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        var result = _consumer.Consume(_cts.Token);
                        emit(result.Message.Value);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Consume loop canceled
                }
                finally
                {
                    _consumer.Close();
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
