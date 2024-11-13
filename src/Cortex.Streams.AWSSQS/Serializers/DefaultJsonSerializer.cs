using System.Text.Json;

namespace Cortex.Streams.AWSSQS.Serializers
{
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
