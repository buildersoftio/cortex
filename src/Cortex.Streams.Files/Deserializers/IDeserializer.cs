namespace Cortex.Streams.Files.Deserializers
{
    /// <summary>
    /// Interface for deserializing strings into objects of type T.
    /// </summary>
    /// <typeparam name="T">The target type for deserialization.</typeparam>
    public interface IDeserializer<T>
    {
        T Deserialize(string input);
    }
}
