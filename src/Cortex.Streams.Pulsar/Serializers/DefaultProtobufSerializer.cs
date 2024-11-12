using Google.Protobuf;

namespace Cortex.Streams.Pulsar.Serializers
{
    public class DefaultProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
    {
        public byte[] Serialize(T data)
        {
            return data.ToByteArray();
        }
    }
}

