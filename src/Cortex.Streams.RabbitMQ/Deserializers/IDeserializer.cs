namespace Cortex.Streams.RabbitMq.Deserializers
{
    /// <summary>
    /// Interface for deserializing strings to objects of type T.
    /// </summary>
    /// <typeparam name="T">The type of object to deserialize to.</typeparam>
    public interface IDeserializer<T>
    {
        /// <summary>
        /// Deserializes a string to an object of type T.
        /// </summary>
        /// <param name="data">The string data to deserialize.</param>
        /// <returns>The deserialized object of type T.</returns>
        T Deserialize(string data);
    }
}
