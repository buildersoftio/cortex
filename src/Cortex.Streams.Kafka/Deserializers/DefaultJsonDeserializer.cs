using Confluent.Kafka;
using System;
using System.Runtime.Serialization;
using System.Text.Json;

namespace Cortex.Streams.Kafka.Deserializers
{
    public class DefaultJsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public DefaultJsonDeserializer(JsonSerializerOptions options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                // Add other default options if necessary
            };
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.IsEmpty)
                return default;

            try
            {
                return JsonSerializer.Deserialize<T>(data, _options)!;
            }
            catch (JsonException ex)
            {
                throw new SerializationException($"JSON Deserialization failed: {ex.Message}", ex);
            }
        }
    }
}
