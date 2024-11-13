using Cortex.Streams.Operators;
using Cortex.Streams.RabbitMq.Deserializers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Cortex.Streams.RabbitMq
{
    /// <summary>
    /// RabbitMQ Source Operator with deserialization support.
    /// </summary>
    /// <typeparam name="TOutput">The type of objects to emit.</typeparam>
    public class RabbitMQSourceOperator<TOutput> : ISourceOperator<TOutput>, IDisposable
    {

        public void Start(Action<TOutput> emit)
        {
            throw new NotImplementedException();
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
