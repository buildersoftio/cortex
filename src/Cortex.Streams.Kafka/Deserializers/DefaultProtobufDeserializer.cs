using Confluent.Kafka;
using ProtoBuf;
using System;
using System.IO;
using System.Runtime.Serialization;

namespace Cortex.Streams.Kafka.Deserializers
{
    public class DefaultProtobufDeserializer<T> : IDeserializer<T> where T : class
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, Confluent.Kafka.SerializationContext context)
        {
            if (isNull || data.IsEmpty)
                return null;

            try
            {
                using (var memoryStream = new MemoryStream(data.ToArray()))
                {
                    return Serializer.Deserialize<T>(memoryStream);
                }
            }
            catch (ProtoException ex)
            {
                throw new SerializationException($"Protobuf Deserialization failed: {ex.Message}", ex);
            }
        }
    }
}
