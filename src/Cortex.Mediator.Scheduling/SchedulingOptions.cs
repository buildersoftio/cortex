using System;

namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// Options for scheduled command processing.
    /// </summary>
    public sealed class SchedulingOptions
    {
        /// <summary>How often the hosted processor looks for due commands. Default: 5 seconds.</summary>
        public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>The maximum number of commands claimed per poll. Default: 100.</summary>
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// How many times a command is attempted before it is marked failed. Default: 3.
        /// A value of 1 disables retries.
        /// </summary>
        public int MaxAttempts { get; set; } = 3;

        /// <summary>How long to wait before retrying a command whose handler threw. Default: 30 seconds.</summary>
        public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// How long a claimed command stays locked to one processor. If the processor stops before
        /// finishing, another processor may claim the command again after this time. Set it longer than
        /// your slowest handler. Default: 5 minutes.
        /// </summary>
        public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Whether to register <see cref="ScheduledCommandProcessor"/> as a hosted service. Default: <c>true</c>.
        /// Set it to <c>false</c> to drive processing yourself, for example from a Hangfire or Quartz.NET job,
        /// by calling <see cref="ScheduledCommandProcessor.ProcessDueCommandsAsync"/>.
        /// </summary>
        public bool EnableProcessor { get; set; } = true;

        /// <summary>Throws <see cref="ArgumentOutOfRangeException"/> if any value is out of range.</summary>
        public void Validate()
        {
            if (PollingInterval <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(PollingInterval), PollingInterval, "The polling interval must be positive.");
            if (BatchSize < 1)
                throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "The batch size must be at least 1.");
            if (MaxAttempts < 1)
                throw new ArgumentOutOfRangeException(nameof(MaxAttempts), MaxAttempts, "The maximum number of attempts must be at least 1.");
            if (RetryDelay < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(RetryDelay), RetryDelay, "The retry delay must not be negative.");
            if (LockDuration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(LockDuration), LockDuration, "The lock duration must be positive.");
        }
    }
}
