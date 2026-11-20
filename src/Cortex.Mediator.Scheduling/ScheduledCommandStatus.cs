namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// The lifecycle state of a scheduled command.
    /// </summary>
    public enum ScheduledCommandStatus
    {
        /// <summary>Waiting until it is due.</summary>
        Pending = 0,

        /// <summary>Claimed by a processor and being dispatched.</summary>
        Processing = 1,

        /// <summary>Dispatched successfully.</summary>
        Completed = 2,

        /// <summary>Dispatch failed on every allowed attempt.</summary>
        Failed = 3,

        /// <summary>Cancelled before it was dispatched.</summary>
        Cancelled = 4
    }
}
