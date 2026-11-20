using System;

namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// A command stored for deferred execution, with its scheduling state.
    /// </summary>
    public sealed class ScheduledCommand
    {
        /// <summary>The schedule identifier returned to the caller.</summary>
        public string Id { get; set; }

        /// <summary>The serialized command type name, as produced by the command serializer.</summary>
        public string CommandType { get; set; }

        /// <summary>The serialized command.</summary>
        public string Payload { get; set; }

        /// <summary>When the command becomes due.</summary>
        public DateTimeOffset ExecuteAt { get; set; }

        /// <summary>When the command was scheduled.</summary>
        public DateTimeOffset CreatedAt { get; set; }

        /// <summary>The current lifecycle state.</summary>
        public ScheduledCommandStatus Status { get; set; }

        /// <summary>How many times a processor has claimed the command for dispatch.</summary>
        public int Attempts { get; set; }

        /// <summary>The error message of the last failed attempt, if any.</summary>
        public string LastError { get; set; }

        /// <summary>While <see cref="Status"/> is <see cref="ScheduledCommandStatus.Processing"/>, the time after which another processor may claim it again.</summary>
        public DateTimeOffset? LockedUntil { get; set; }

        /// <summary>Creates a copy of this instance.</summary>
        public ScheduledCommand Clone() => (ScheduledCommand)MemberwiseClone();
    }
}
