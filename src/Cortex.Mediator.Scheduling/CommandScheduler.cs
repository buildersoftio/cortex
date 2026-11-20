using Cortex.Mediator.Commands;
using Cortex.Mediator.Scheduling.Serialization;
using Cortex.Mediator.Scheduling.Stores;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// Default <see cref="ICommandScheduler"/>: serializes the command and writes it to the configured store,
    /// where <c>ScheduledCommandProcessor</c> picks it up when it is due.
    /// </summary>
    public sealed class CommandScheduler : ICommandScheduler
    {
        private readonly IScheduledCommandStore _store;
        private readonly IScheduledCommandSerializer _serializer;
        private readonly TimeProvider _timeProvider;

        /// <summary>Creates a scheduler over the given store, serializer and clock.</summary>
        public CommandScheduler(IScheduledCommandStore store, IScheduledCommandSerializer serializer, TimeProvider timeProvider)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        }

        /// <inheritdoc />
        public Task<string> ScheduleAsync<TCommand>(TCommand command, TimeSpan delay, CancellationToken ct = default)
            where TCommand : ICommand
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));
            if (delay < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(delay), delay, "The delay must not be negative.");

            return ScheduleCoreAsync(command, _timeProvider.GetUtcNow() + delay, ct);
        }

        /// <inheritdoc />
        public Task<string> ScheduleAsync<TCommand>(TCommand command, DateTimeOffset executeAt, CancellationToken ct = default)
            where TCommand : ICommand
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));

            return ScheduleCoreAsync(command, executeAt, ct);
        }

        /// <inheritdoc />
        public Task CancelAsync(string scheduleId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(scheduleId))
                throw new ArgumentException("The schedule id must not be empty.", nameof(scheduleId));

            return _store.CancelAsync(scheduleId, ct);
        }

        private async Task<string> ScheduleCoreAsync(ICommand command, DateTimeOffset executeAt, CancellationToken ct)
        {
            var scheduled = new ScheduledCommand
            {
                Id = Guid.NewGuid().ToString("N"),
                CommandType = _serializer.GetTypeName(command),
                Payload = _serializer.Serialize(command),
                ExecuteAt = executeAt,
                CreatedAt = _timeProvider.GetUtcNow(),
                Status = ScheduledCommandStatus.Pending,
                Attempts = 0
            };

            await _store.AddAsync(scheduled, ct).ConfigureAwait(false);
            return scheduled.Id;
        }
    }
}
