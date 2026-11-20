using Cortex.Mediator.Commands;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// Schedules commands for deferred execution through the mediator.
    /// </summary>
    public interface ICommandScheduler
    {
        /// <summary>
        /// Schedules a command to be sent after the given delay.
        /// </summary>
        /// <typeparam name="TCommand">The type of command to schedule.</typeparam>
        /// <param name="command">The command to schedule. It must be serializable by the configured serializer.</param>
        /// <param name="delay">How long to wait before the command becomes due. Must not be negative.</param>
        /// <param name="ct">The cancellation token.</param>
        /// <returns>The identifier of the scheduled command, usable with <see cref="CancelAsync"/>.</returns>
        Task<string> ScheduleAsync<TCommand>(TCommand command, TimeSpan delay, CancellationToken ct = default)
            where TCommand : ICommand;

        /// <summary>
        /// Schedules a command to be sent at the given point in time.
        /// A time in the past makes the command due immediately.
        /// </summary>
        /// <typeparam name="TCommand">The type of command to schedule.</typeparam>
        /// <param name="command">The command to schedule. It must be serializable by the configured serializer.</param>
        /// <param name="executeAt">When the command becomes due.</param>
        /// <param name="ct">The cancellation token.</param>
        /// <returns>The identifier of the scheduled command, usable with <see cref="CancelAsync"/>.</returns>
        Task<string> ScheduleAsync<TCommand>(TCommand command, DateTimeOffset executeAt, CancellationToken ct = default)
            where TCommand : ICommand;

        /// <summary>
        /// Cancels a scheduled command that has not started executing yet.
        /// Cancelling an unknown, already executed or already cancelled command has no effect.
        /// </summary>
        /// <param name="scheduleId">The identifier returned by <c>ScheduleAsync</c>.</param>
        /// <param name="ct">The cancellation token.</param>
        Task CancelAsync(string scheduleId, CancellationToken ct = default);
    }
}
