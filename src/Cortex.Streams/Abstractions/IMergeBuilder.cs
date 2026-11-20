using System;

namespace Cortex.Streams.Abstractions
{
    /// <summary>
    /// Configures the branches of a merge. Every branch receives the items at the merge point,
    /// transforms them to <typeparamref name="TNext"/>, and its output joins one combined stream.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The type of the items entering each branch.</typeparam>
    /// <typeparam name="TNext">The type of the merged items.</typeparam>
    public interface IMergeBuilder<TIn, TCurrent, TNext>
    {
        /// <summary>
        /// Adds a branch whose output joins the merged stream.
        /// </summary>
        /// <param name="name">The unique name of the branch.</param>
        /// <param name="config">
        /// Configures the branch and returns the builder at its end, for example <c>b =&gt; b.Map(x =&gt; x.ToString())</c>.
        /// Return the given builder unchanged (<c>b =&gt; b</c>) to pass items through.
        /// </param>
        /// <returns>The merge builder for method chaining.</returns>
        /// <exception cref="ArgumentException">The name is null, empty or already used in this merge, or <paramref name="config"/> returned a builder it did not derive from the one it was given.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
        IMergeBuilder<TIn, TCurrent, TNext> Branch(
            string name,
            Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config);

        /// <summary>
        /// Adds a conditional branch: only items matching <paramref name="condition"/> enter it, and its output joins the merged stream.
        /// </summary>
        /// <param name="name">The unique name of the branch.</param>
        /// <param name="condition">A predicate deciding which items enter the branch. It follows the stream's error handling.</param>
        /// <param name="config">Configures the branch and returns the builder at its end.</param>
        /// <returns>The merge builder for method chaining.</returns>
        /// <exception cref="ArgumentException">The name is null, empty or already used in this merge, or <paramref name="config"/> returned a builder it did not derive from the one it was given.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="condition"/> or <paramref name="config"/> is null.</exception>
        IMergeBuilder<TIn, TCurrent, TNext> Branch(
            string name,
            Func<TCurrent, bool> condition,
            Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config);
    }
}
