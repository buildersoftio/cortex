using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>
    /// Persists scheduled commands and hands due ones to processors.
    /// Implementations must make <see cref="ClaimDueAsync"/> atomic per command so that
    /// concurrent processors never claim the same command at the same time.
    /// </summary>
    public interface IScheduledCommandStore
    {
        /// <summary>Stores a new scheduled command.</summary>
        Task AddAsync(ScheduledCommand command, CancellationToken ct = default);

        /// <summary>Returns the scheduled command with the given id, or <c>null</c> if it does not exist.</summary>
        Task<ScheduledCommand> GetAsync(string id, CancellationToken ct = default);

        /// <summary>
        /// Cancels a pending command.
        /// Returns <c>true</c> if the command was pending and is now cancelled; otherwise <c>false</c>.
        /// </summary>
        Task<bool> CancelAsync(string id, CancellationToken ct = default);

        /// <summary>
        /// Claims up to <paramref name="maxCount"/> commands that are due at <paramref name="now"/>:
        /// pending commands whose execution time has passed, and processing commands whose lock has expired.
        /// Each claimed command is marked processing, locked until <paramref name="now"/> + <paramref name="lockDuration"/>,
        /// and its attempt count is incremented. Commands are returned in execution-time order.
        /// </summary>
        Task<IReadOnlyList<ScheduledCommand>> ClaimDueAsync(DateTimeOffset now, int maxCount, TimeSpan lockDuration, CancellationToken ct = default);

        /// <summary>Marks a claimed command as completed.</summary>
        Task MarkCompletedAsync(string id, CancellationToken ct = default);

        /// <summary>
        /// Records a failed attempt. When <paramref name="retryAt"/> has a value the command becomes pending again,
        /// due at that time; otherwise it is marked failed for good.
        /// </summary>
        Task MarkFailedAsync(string id, string error, DateTimeOffset? retryAt, CancellationToken ct = default);
    }
}
