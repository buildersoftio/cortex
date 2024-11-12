using System;
using System.Buffers;
using System.Text;

namespace Cortex.Streams.Pulsar.Deserializers
{
    public class DefaultBase64Deserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySequence<byte> data)
        {
            var base64String = Encoding.UTF8.GetString(data);
            var bytes = Convert.FromBase64String(base64String);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
