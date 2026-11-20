using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling.Stores
{
    /// <summary>
    /// A process-local, non-durable store for development and testing.
    /// Scheduled commands are lost when the process stops, and the store is not shared between processes.
    /// </summary>
    public sealed class InMemoryScheduledCommandStore : IScheduledCommandStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, ScheduledCommand> _commands = new(StringComparer.Ordinal);

        /// <inheritdoc />
        public Task AddAsync(ScheduledCommand command, CancellationToken ct = default)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));
            if (string.IsNullOrWhiteSpace(command.Id))
                throw new ArgumentException("The scheduled command must have an id.", nameof(command));

            lock (_gate)
            {
                if (_commands.ContainsKey(command.Id))
                    throw new InvalidOperationException($"A scheduled command with id '{command.Id}' already exists.");

                _commands.Add(command.Id, command.Clone());
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task<ScheduledCommand> GetAsync(string id, CancellationToken ct = default)
        {
            lock (_gate)
            {
                return Task.FromResult(id != null && _commands.TryGetValue(id, out var command) ? command.Clone() : null);
            }
        }

        /// <inheritdoc />
        public Task<bool> CancelAsync(string id, CancellationToken ct = default)
        {
            lock (_gate)
            {
                if (id == null || !_commands.TryGetValue(id, out var command) || command.Status != ScheduledCommandStatus.Pending)
                    return Task.FromResult(false);

                command.Status = ScheduledCommandStatus.Cancelled;
                return Task.FromResult(true);
            }
        }

        /// <inheritdoc />
        public Task<IReadOnlyList<ScheduledCommand>> ClaimDueAsync(DateTimeOffset now, int maxCount, TimeSpan lockDuration, CancellationToken ct = default)
        {
            if (maxCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxCount), "The batch size must be positive.");

            lock (_gate)
            {
                var claimed = _commands.Values
                    .Where(c => IsClaimable(c, now))
                    .OrderBy(c => c.ExecuteAt)
                    .ThenBy(c => c.Id, StringComparer.Ordinal)
                    .Take(maxCount)
                    .ToList();

                foreach (var command in claimed)
                {
                    command.Status = ScheduledCommandStatus.Processing;
                    command.LockedUntil = now + lockDuration;
                    command.Attempts++;
                }

                IReadOnlyList<ScheduledCommand> result = claimed.Select(c => c.Clone()).ToList();
                return Task.FromResult(result);
            }
        }

        /// <inheritdoc />
        public Task MarkCompletedAsync(string id, CancellationToken ct = default)
        {
            lock (_gate)
            {
                if (id != null && _commands.TryGetValue(id, out var command))
                {
                    command.Status = ScheduledCommandStatus.Completed;
                    command.LockedUntil = null;
                }
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task MarkFailedAsync(string id, string error, DateTimeOffset? retryAt, CancellationToken ct = default)
        {
            lock (_gate)
            {
                if (id != null && _commands.TryGetValue(id, out var command))
                {
                    command.LastError = error;
                    command.LockedUntil = null;

                    if (retryAt.HasValue)
                    {
                        command.Status = ScheduledCommandStatus.Pending;
                        command.ExecuteAt = retryAt.Value;
                    }
                    else
                    {
                        command.Status = ScheduledCommandStatus.Failed;
                    }
                }
            }

            return Task.CompletedTask;
        }

        private static bool IsClaimable(ScheduledCommand command, DateTimeOffset now)
        {
            return (command.Status == ScheduledCommandStatus.Pending && command.ExecuteAt <= now)
                || (command.Status == ScheduledCommandStatus.Processing && command.LockedUntil.HasValue && command.LockedUntil.Value <= now);
        }
    }
}
