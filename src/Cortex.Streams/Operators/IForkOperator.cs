using Cortex.Streams.Abstractions;
using System.Collections.Generic;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// Non-generic view of a fork point, used by <see cref="Stream{TIn, TCurrent}"/> to
    /// list and remove branches without knowing the fork's element type.
    /// </summary>
    internal interface IForkOperator
    {
        /// <summary>
        /// Gets a snapshot of the branches currently attached to the fork, in insertion order.
        /// </summary>
        IReadOnlyList<IBranchInfo> GetBranchInfos();

        /// <summary>
        /// Removes the branch with the given name.
        /// </summary>
        /// <returns>The removed branch operator, or null if the fork has no branch with this name.</returns>
        IOperator RemoveBranch(string name);
    }
}
