namespace Cortex.Streams.Files.Serializers
{

    /// <summary>
    /// Interface for serializing objects of type T into strings.
    /// </summary>
    /// <typeparam name="T">The source type for serialization.</typeparam>
    public interface ISerializer<T>
    {
        string Serialize(T input);
    }
}
