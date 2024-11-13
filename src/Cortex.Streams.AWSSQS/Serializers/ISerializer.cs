namespace Cortex.Streams.AWSSQS.Serializers
{
    /// <summary>
    /// Interface for serializing objects of type T to strings.
    /// </summary>
    /// <typeparam name="T">The type of object to serialize.</typeparam>
    public interface ISerializer<T>
    {
        string Serialize(T obj);
    }
}
