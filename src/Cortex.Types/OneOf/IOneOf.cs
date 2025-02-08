using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Types
{
    /// <summary>
    /// Base interface for all OneOf types providing common functionality
    /// </summary>
    public interface IOneOf
    {
        /// <summary>
        /// Gets the boxed value stored in the OneOf container
        /// </summary>
        object Value { get; }

        /// <summary>
        /// Gets the 0-based index of the declared type parameter
        /// </summary>
        int TypeIndex { get; }
    }
}
