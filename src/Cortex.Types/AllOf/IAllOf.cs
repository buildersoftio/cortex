namespace Cortex.Types
{
    /// <summary>
    /// Base interface for all AllOf types providing common functionality
    /// </summary>
    public interface IAllOf
    {
        /// <summary>
        /// Gets the boxed value stored in the AllOf container
        /// </summary>
        object Value { get; }
    }
}
