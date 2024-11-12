using Confluent.Kafka;
using System;
using System.Text;

namespace Cortex.Streams.Kafka.Serializers
{
    public class DefaultBase64Serializer : ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            var bytes = Encoding.UTF8.GetBytes(data);
            var base64String = Convert.ToBase64String(bytes);
            return Encoding.UTF8.GetBytes(base64String);
        }
    }
}
