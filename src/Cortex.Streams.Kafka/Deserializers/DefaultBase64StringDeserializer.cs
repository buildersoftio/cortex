using Confluent.Kafka;
using System;
using System.Runtime.Serialization;

namespace Cortex.Streams.Kafka.Deserializers
{
    public class DefaultBase64StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.IsEmpty)
                return null;

            // Convert byte array to string (assuming UTF-8 encoding)
            string base64String = System.Text.Encoding.UTF8.GetString(data);

            try
            {
                // Decode Base64 string to bytes
                byte[] bytes = Convert.FromBase64String(base64String);
                // Convert bytes back to string (assuming UTF-8)
                return System.Text.Encoding.UTF8.GetString(bytes);
            }
            catch (FormatException ex)
            {
                throw new SerializationException($"Invalid Base64 string: {ex.Message}", ex);
            }
        }
    }
}
