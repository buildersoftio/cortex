using System.Linq;

namespace Cortex.Streams.Files.Serializers
{
    /// <summary>
    /// Default CSV serializer. Writes properties as comma-separated values.
    /// </summary>
    /// <typeparam name="T">The source type for serialization.</typeparam>
    public class DefaultCsvSerializer<T> : ISerializer<T>
    {
        public string Serialize(T input)
        {
            var properties = typeof(T).GetProperties();
            var values = properties.Select(p => p.GetValue(input)?.ToString() ?? string.Empty);
            return string.Join(",", values);
        }
    }
}
