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
        /// <returns>True if a branch was removed; otherwise false.</returns>
        bool RemoveBranch(string name);
    }
}
