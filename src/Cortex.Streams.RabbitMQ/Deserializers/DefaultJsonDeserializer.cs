using System.Text.Json;

namespace Cortex.Streams.RabbitMq.Deserializers
{
    /// <summary>
    /// JSON implementation of IDeserializer.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    public class DefaultJsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public DefaultJsonDeserializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        public T Deserialize(string data)
        {
            return JsonSerializer.Deserialize<T>(data, _options);
        }
    }
}
