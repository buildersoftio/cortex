namespace Cortex.Streams.AWSSQS.Deserializers
{
    /// <summary>
    /// Interface for deserializing strings to objects of type T.
    /// </summary>
    /// <typeparam name="T">The type of object to deserialize to.</typeparam>
    public interface IDeserializer<T>
    {
        T Deserialize(string data);
    }
}
