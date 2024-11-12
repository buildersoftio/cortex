namespace Cortex.Streams.Pulsar.Serializers
{
    public interface ISerializer<T>
    {
        byte[] Serialize(T data);
    }
}
