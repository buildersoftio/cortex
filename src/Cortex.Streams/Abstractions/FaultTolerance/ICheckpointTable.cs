namespace Cortex.Streams.Abstractions.FaultTolerance
{
    /// <summary>
    /// Implementing classes can take and restore checkpoints of their internal state.
    /// </summary>
    public interface ICheckpointTable
    {
        /// <summary>
        /// Saves the current operator state to a checkpoint store.
        /// </summary>
        void Checkpoint();

        /// <summary>
        /// Restores the operator state from a previously saved checkpoint, if present.
        /// </summary>
        void RestoreCheckpoint();
    }
}
