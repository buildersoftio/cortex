using Google.Protobuf;
using System.Buffers;

namespace Cortex.Streams.Pulsar.Deserializers
{
    public class DefaultProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
    {
        public T Deserialize(ReadOnlySequence<byte> data)
        {
            var parser = new MessageParser<T>(() => new T());
            return parser.ParseFrom(data);
        }
    }
}
