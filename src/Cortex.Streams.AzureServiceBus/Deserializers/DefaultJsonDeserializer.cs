using System.Text.Json;

namespace Cortex.Streams.AzureServiceBus.Deserializers
{
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
            return JsonSerializer.Deserialize<T>(data, _options)!;
        }
    }
}
