using System.Text.Json;

namespace Cortex.Streams.RabbitMq.Serializers
{
    /// <summary>
    /// JSON implementation of ISerializer.
    /// </summary>
    /// <typeparam name="T">The type to serialize.</typeparam>
    public class DefaultJsonSerializer<T> : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public DefaultJsonSerializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
        }

        public string Serialize(T obj)
        {
            return JsonSerializer.Serialize(obj, _options);
        }
    }
}
