using System.Linq;
using System.Xml.Linq;

namespace Cortex.Streams.Files.Serializers
{
    /// <summary>
    /// Default XML serializer using System.Xml.Linq.
    /// </summary>
    /// <typeparam name="T">The source type for serialization.</typeparam>
    public class DefaultXmlSerializer<T> : ISerializer<T>
    {
        public string Serialize(T input)
        {
            var xElement = new XElement(typeof(T).Name,
                typeof(T).GetProperties().Select(prop =>
                    new XElement(prop.Name, prop.GetValue(input)?.ToString() ?? string.Empty)));
            return xElement.ToString();
        }
    }
}
