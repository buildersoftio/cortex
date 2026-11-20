using Cortex.Mediator.Commands;
using Cortex.Mediator.Scheduling.Serialization;
using Cortex.Mediator.Scheduling.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Scheduling
{
    /// <summary>
    /// Polls the <see cref="IScheduledCommandStore"/> for due commands and sends each one through
    /// <see cref="IMediator"/> in its own dependency-injection scope.
    /// Runs as a hosted service, or can be driven manually through <see cref="ProcessDueCommandsAsync"/>.
    /// Delivery is at-least-once: a command can run again if the process stops after its handler
    /// finished but before the command was marked completed, so handlers should be idempotent.
    /// </summary>
    public class ScheduledCommandProcessor : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IScheduledCommandStore _store;
        private readonly IScheduledCommandSerializer _serializer;
        private readonly SchedulingOptions _options;
        private readonly TimeProvider _timeProvider;
        private readonly ILogger<ScheduledCommandProcessor> _logger;

        /// <summary>Creates a processor.</summary>
        public ScheduledCommandProcessor(
            IServiceScopeFactory scopeFactory,
            IScheduledCommandStore store,
            IScheduledCommandSerializer serializer,
            SchedulingOptions options,
            TimeProvider timeProvider,
            ILogger<ScheduledCommandProcessor> logger = null)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _store = store ?? throw new ArgumentNullException(nameof(store));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
            _logger = logger ?? NullLogger<ScheduledCommandProcessor>.Instance;
            _options.Validate();
        }

        /// <summary>
        /// Claims up to <see cref="SchedulingOptions.BatchSize"/> due commands and dispatches them one by one.
        /// A failing command is retried or marked failed and does not stop the rest of the batch.
        /// </summary>
        /// <param name="ct">Stops processing; commands claimed but not finished are offered again after the lock expires.</param>
        /// <returns>The number of commands claimed in this call.</returns>
        public async Task<int> ProcessDueCommandsAsync(CancellationToken ct = default)
        {
            var now = _timeProvider.GetUtcNow();
            var claimed = await _store.ClaimDueAsync(now, _options.BatchSize, _options.LockDuration, ct).ConfigureAwait(false);

            foreach (var scheduled in claimed)
            {
                ct.ThrowIfCancellationRequested();
                await DispatchAsync(scheduled, ct).ConfigureAwait(false);
            }

            return claimed.Count;
        }

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    int claimed;
                    do
                    {
                        claimed = await ProcessDueCommandsAsync(stoppingToken).ConfigureAwait(false);
                    }
                    while (claimed >= _options.BatchSize && !stoppingToken.IsCancellationRequested);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Polling for scheduled commands failed; retrying in {PollingInterval}.", _options.PollingInterval);
                }

                try
                {
                    await Task.Delay(_options.PollingInterval, _timeProvider, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        private async Task DispatchAsync(ScheduledCommand scheduled, CancellationToken ct)
        {
            if (scheduled.Attempts > _options.MaxAttempts)
            {
                var error = $"Scheduled command was claimed {scheduled.Attempts} times, more than the {_options.MaxAttempts} allowed attempts; " +
                            "a processor probably stopped while running it.";
                _logger.LogError("Scheduled command {ScheduleId} ({CommandType}) failed: {Error}", scheduled.Id, scheduled.CommandType, error);
                await _store.MarkFailedAsync(scheduled.Id, error, null, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            ICommand command;
            try
            {
                command = _serializer.Deserialize(scheduled.CommandType, scheduled.Payload);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Scheduled command {ScheduleId} ({CommandType}) could not be read and will not be retried.", scheduled.Id, scheduled.CommandType);
                await _store.MarkFailedAsync(scheduled.Id, ex.Message, null, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            try
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                    await mediator.SendCommandAsync(command, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                DateTimeOffset? retryAt = scheduled.Attempts < _options.MaxAttempts
                    ? _timeProvider.GetUtcNow() + _options.RetryDelay
                    : (DateTimeOffset?)null;

                if (retryAt.HasValue)
                    _logger.LogWarning(ex, "Scheduled command {ScheduleId} ({CommandType}) failed on attempt {Attempt} of {MaxAttempts}; retrying at {RetryAt}.",
                        scheduled.Id, scheduled.CommandType, scheduled.Attempts, _options.MaxAttempts, retryAt.Value);
                else
                    _logger.LogError(ex, "Scheduled command {ScheduleId} ({CommandType}) failed on its last attempt ({Attempt}).",
                        scheduled.Id, scheduled.CommandType, scheduled.Attempts);

                await _store.MarkFailedAsync(scheduled.Id, ex.Message, retryAt, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            await _store.MarkCompletedAsync(scheduled.Id, CancellationToken.None).ConfigureAwait(false);
        }
    }
}
