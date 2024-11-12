using Newtonsoft.Json;
using System.Text;

namespace Cortex.Streams.Pulsar.Serializers
{
    public class DefaultJsonSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data)
        {
            var json = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}
