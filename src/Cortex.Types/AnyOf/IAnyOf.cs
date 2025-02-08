using System.Collections.Generic;

namespace Cortex.Types
{
    /// <summary>
    /// Base interface for all AnyOf types providing common functionality
    /// </summary>
    public interface IAnyOf
    {
        /// <summary>
        /// Gets the boxed value stored in the AnyOf container
        /// </summary>
        object Value { get; }

        /// <summary>
        /// Gets the indices of the declared type parameters that match the stored value
        /// </summary>
        IEnumerable<int> TypeIndices { get; }
    }
}
