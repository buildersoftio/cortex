using System.Text.Json;

namespace Cortex.Streams.Files.Deserializers
{
    /// <summary>
    /// Default JSON deserializer using System.Text.Json.
    /// </summary>
    /// <typeparam name="T">The target type for deserialization.</typeparam>
    public class DefaultJsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(string input)
        {
            return JsonSerializer.Deserialize<T>(input)!;
        }
    }
}
