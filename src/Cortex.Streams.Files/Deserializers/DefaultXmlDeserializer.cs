using System;
using System.Xml.Linq;

namespace Cortex.Streams.Files.Deserializers
{
    /// <summary>
    /// Default XML deserializer using System.Xml.Linq.
    /// </summary>
    /// <typeparam name="T">The target type for deserialization.</typeparam>
    public class DefaultXmlDeserializer<T> : IDeserializer<T> where T : new()
    {
        public T Deserialize(string input)
        {
            var xElement = XElement.Parse(input);
            var obj = new T();
            foreach (var prop in typeof(T).GetProperties())
            {
                var element = xElement.Element(prop.Name);
                if (element != null)
                {
                    var value = Convert.ChangeType(element.Value, prop.PropertyType);
                    prop.SetValue(obj, value);
                }
            }
            return obj;
        }
    }
}
