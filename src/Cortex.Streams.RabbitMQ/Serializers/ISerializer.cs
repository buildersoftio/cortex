namespace Cortex.Streams.RabbitMq.Serializers
{
    /// <summary>
    /// Interface for serializing objects of type T to strings.
    /// </summary>
    /// <typeparam name="T">The type of object to serialize.</typeparam>
    public interface ISerializer<T>
    {
        /// <summary>
        /// Serializes an object of type T to a string.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>The serialized string representation of the object.</returns>
        string Serialize(T obj);
    }
}
