using System;
using System.Text;

namespace Cortex.Streams.Pulsar.Serializers
{
    public class DefaultBase64Serializer : ISerializer<string>
    {
        public byte[] Serialize(string data)
        {
            var bytes = Encoding.UTF8.GetBytes(data);
            var base64String = Convert.ToBase64String(bytes);
            return Encoding.UTF8.GetBytes(base64String);
        }
    }
}
