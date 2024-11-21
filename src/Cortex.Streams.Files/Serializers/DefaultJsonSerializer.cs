using System.Text.Json;

namespace Cortex.Streams.Files.Serializers
{
    /// <summary>
    /// Default JSON serializer using System.Text.Json.
    /// </summary>
    /// <typeparam name="T">The source type for serialization.</typeparam>
    public class DefaultJsonSerializer<T> : ISerializer<T>
    {
        public string Serialize(T input)
        {
            return JsonSerializer.Serialize(input);
        }
    }
}
