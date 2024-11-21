using System;
using System.Linq;

namespace Cortex.Streams.Files.Deserializers
{
    /// <summary>
    /// Default CSV deserializer. Assumes the first line contains headers matching property names of T.
    /// </summary>
    /// <typeparam name="T">The target type for deserialization.</typeparam>
    public class DefaultCsvDeserializer<T> : IDeserializer<T> where T : new()
    {
        private static string[] _headers = null;

        public T Deserialize(string input)
        {
            if (_headers == null)
            {
                // This assumes that the first call will set the headers. In a real implementation, headers should be set externally.
                throw new InvalidOperationException("Headers not set. Initialize headers before deserialization.");
            }

            var values = input.Split(',');
            var obj = new T();
            var properties = typeof(T).GetProperties();

            for (int i = 0; i < Math.Min(_headers.Length, values.Length); i++)
            {
                var prop = properties.FirstOrDefault(p => p.Name.Equals(_headers[i], StringComparison.OrdinalIgnoreCase));
                if (prop != null)
                {
                    var value = Convert.ChangeType(values[i], prop.PropertyType);
                    prop.SetValue(obj, value);
                }
            }

            return obj;
        }

        /// <summary>
        /// Initializes headers for CSV deserialization.
        /// Should be called once with the header line.
        /// </summary>
        /// <param name="headerLine">The header line from the CSV file.</param>
        public void InitializeHeaders(string headerLine)
        {
            _headers = headerLine.Split(',');
        }
    }
}
