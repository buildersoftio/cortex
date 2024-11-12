using Newtonsoft.Json;
using System.Buffers;
using System.Text;

namespace Cortex.Streams.Pulsar.Deserializers
{
    public class DefaultJsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySequence<byte> data)
        {
            var json = Encoding.UTF8.GetString(data);
            return JsonConvert.DeserializeObject<T>(json)!;
        }
    }
}
