using System.Buffers;

namespace Cortex.Streams.Pulsar.Deserializers
{
    public interface IDeserializer<T>
    {
        T Deserialize(ReadOnlySequence<byte> data);
    }
}
