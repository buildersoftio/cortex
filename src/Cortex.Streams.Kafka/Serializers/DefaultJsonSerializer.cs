using Confluent.Kafka;
using System;
using System.Runtime.Serialization;
using System.Text.Json;

namespace Cortex.Streams.Kafka.Serializers
{
    public class DefaultJsonSerializer<T> : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public DefaultJsonSerializer(JsonSerializerOptions options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                // Add other default options if necessary
            };
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null)
                return Array.Empty<byte>();

            try
            {
                return JsonSerializer.SerializeToUtf8Bytes(data, _options);
            }
            catch (JsonException ex)
            {
                throw new SerializationException($"JSON Serialization failed: {ex.Message}", ex);
            }
        }
    }
}
