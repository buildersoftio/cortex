using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Scheduling;
using Cortex.Mediator.Scheduling.DependencyInjection;
using Cortex.Mediator.Scheduling.Stores;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public class ScheduledCommandProcessorTests
    {
        public class SendReminderCommand : ICommand
        {
            public string Email { get; set; } = string.Empty;
        }

        public class FailingCommand : ICommand
        {
            public string Reason { get; set; } = string.Empty;
        }

        public class CancellingCommand : ICommand
        {
        }

        public class SlowCommand : ICommand
        {
            public string Name { get; set; } = string.Empty;
        }

        public sealed class Recorder
        {
            public ConcurrentQueue<string> Handled { get; } = new();
            public CancellationTokenSource Stop { get; } = new();
            public FakeTimeProvider? Clock { get; set; }
            public Func<Task>? AfterSecondSlowCommand { get; set; }
        }

        public class SendReminderHandler : ICommandHandler<SendReminderCommand>
        {
            private readonly Recorder _recorder;
            public SendReminderHandler(Recorder recorder) => _recorder = recorder;

            public Task Handle(SendReminderCommand command, CancellationToken cancellationToken)
            {
                _recorder.Handled.Enqueue(command.Email);
                return Task.CompletedTask;
            }
        }

        public class FailingHandler : ICommandHandler<FailingCommand>
        {
            public Task Handle(FailingCommand command, CancellationToken cancellationToken)
                => throw new InvalidOperationException(command.Reason);
        }

        public class CancellingHandler : ICommandHandler<CancellingCommand>
        {
            private readonly Recorder _recorder;
            public CancellingHandler(Recorder recorder) => _recorder = recorder;

            public Task Handle(CancellingCommand command, CancellationToken cancellationToken)
            {
                _recorder.Stop.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
        }

        public class SlowHandler : ICommandHandler<SlowCommand>
        {
            private readonly Recorder _recorder;
            public SlowHandler(Recorder recorder) => _recorder = recorder;

            public async Task Handle(SlowCommand command, CancellationToken cancellationToken)
            {
                _recorder.Handled.Enqueue(command.Name);
                _recorder.Clock!.Advance(TimeSpan.FromSeconds(40));

                if (command.Name == "2" && _recorder.AfterSecondSlowCommand != null)
                    await _recorder.AfterSecondSlowCommand();
            }
        }

        private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        private readonly FakeTimeProvider _clock = new(T0);
        private readonly Recorder _recorder = new();

        private ServiceProvider BuildProvider(Action<SchedulingOptions>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddCortexMediator(Array.Empty<Type>());
            services.AddSingleton(_recorder);
            services.AddScoped<ICommandHandler<SendReminderCommand>, SendReminderHandler>();
            services.AddScoped<ICommandHandler<FailingCommand>, FailingHandler>();
            services.AddScoped<ICommandHandler<CancellingCommand>, CancellingHandler>();
            services.AddScoped<ICommandHandler<SlowCommand>, SlowHandler>();
            services.AddSingleton<TimeProvider>(_clock);
            services.AddCortexMediatorScheduling(configure);
            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task DueCommand_IsDispatchedOnce_AndMarkedCompleted()
        {
            using var provider = BuildProvider();
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();

            var id = await scheduler.ScheduleAsync(new SendReminderCommand { Email = "a@b.c" }, TimeSpan.FromMinutes(10));
            _clock.Advance(TimeSpan.FromMinutes(10));

            var first = await processor.ProcessDueCommandsAsync();
            var second = await processor.ProcessDueCommandsAsync();

            Assert.Equal(1, first);
            Assert.Equal(0, second);
            Assert.Equal(new[] { "a@b.c" }, _recorder.Handled);
            Assert.Equal(ScheduledCommandStatus.Completed, (await store.GetAsync(id)).Status);
        }

        [Fact]
        public async Task CommandNotYetDue_IsNotDispatched()
        {
            using var provider = BuildProvider();
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();

            await scheduler.ScheduleAsync(new SendReminderCommand { Email = "later" }, TimeSpan.FromHours(24));
            _clock.Advance(TimeSpan.FromHours(23));

            Assert.Equal(0, await processor.ProcessDueCommandsAsync());
            Assert.Empty(_recorder.Handled);
        }

        [Fact]
        public async Task CancelledCommand_IsNeverDispatched()
        {
            using var provider = BuildProvider();
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();

            var id = await scheduler.ScheduleAsync(new SendReminderCommand { Email = "cancelled" }, TimeSpan.FromMinutes(1));
            await scheduler.CancelAsync(id);
            _clock.Advance(TimeSpan.FromHours(1));

            Assert.Equal(0, await processor.ProcessDueCommandsAsync());
            Assert.Empty(_recorder.Handled);
        }

        [Fact]
        public async Task FailingCommand_IsRetriedAfterRetryDelay_ThenMarkedFailed()
        {
            using var provider = BuildProvider(o =>
            {
                o.MaxAttempts = 2;
                o.RetryDelay = TimeSpan.FromMinutes(1);
            });
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();

            var id = await scheduler.ScheduleAsync(new FailingCommand { Reason = "gateway down" }, TimeSpan.Zero);

            Assert.Equal(1, await processor.ProcessDueCommandsAsync());
            var afterFirst = await store.GetAsync(id);
            Assert.Equal(ScheduledCommandStatus.Pending, afterFirst.Status);
            Assert.Equal(T0.AddMinutes(1), afterFirst.ExecuteAt);
            Assert.Equal("gateway down", afterFirst.LastError);

            _clock.Advance(TimeSpan.FromSeconds(30));
            Assert.Equal(0, await processor.ProcessDueCommandsAsync());

            _clock.Advance(TimeSpan.FromSeconds(30));
            Assert.Equal(1, await processor.ProcessDueCommandsAsync());
            var afterSecond = await store.GetAsync(id);
            Assert.Equal(ScheduledCommandStatus.Failed, afterSecond.Status);
            Assert.Equal(2, afterSecond.Attempts);
            Assert.Equal("gateway down", afterSecond.LastError);

            _clock.Advance(TimeSpan.FromHours(1));
            Assert.Equal(0, await processor.ProcessDueCommandsAsync());
        }

        [Fact]
        public async Task OneFailingCommand_DoesNotStopTheRestOfTheBatch()
        {
            using var provider = BuildProvider(o => o.MaxAttempts = 1);
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();

            await scheduler.ScheduleAsync(new SendReminderCommand { Email = "first" }, TimeSpan.Zero);
            _clock.Advance(TimeSpan.FromSeconds(1));
            await scheduler.ScheduleAsync(new FailingCommand { Reason = "x" }, TimeSpan.Zero);
            _clock.Advance(TimeSpan.FromSeconds(1));
            await scheduler.ScheduleAsync(new SendReminderCommand { Email = "third" }, TimeSpan.Zero);

            Assert.Equal(3, await processor.ProcessDueCommandsAsync());
            Assert.Equal(new[] { "first", "third" }, _recorder.Handled);
        }

        [Fact]
        public async Task UnreadableCommand_IsMarkedFailedWithoutRetry()
        {
            using var provider = BuildProvider(o => o.MaxAttempts = 5);
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();
            await store.AddAsync(new ScheduledCommand
            {
                Id = "bad",
                CommandType = "Does.Not.Exist, Nowhere",
                Payload = "{}",
                ExecuteAt = T0,
                CreatedAt = T0,
                Status = ScheduledCommandStatus.Pending
            });

            Assert.Equal(1, await processor.ProcessDueCommandsAsync());

            var stored = await store.GetAsync("bad");
            Assert.Equal(ScheduledCommandStatus.Failed, stored.Status);
            Assert.Contains("could not be resolved", stored.LastError);
        }

        [Fact]
        public async Task CommandReclaimedAfterLockExpiry_BeyondMaxAttempts_IsFailedWithoutDispatch()
        {
            using var provider = BuildProvider(o =>
            {
                o.MaxAttempts = 1;
                o.LockDuration = TimeSpan.FromMinutes(1);
            });
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();

            var id = await scheduler.ScheduleAsync(new SendReminderCommand { Email = "crashed" }, TimeSpan.Zero);
            // Simulate a processor that claimed the command and then crashed.
            await store.ClaimDueAsync(T0, 10, TimeSpan.FromMinutes(1));
            _clock.Advance(TimeSpan.FromMinutes(2));

            Assert.Equal(1, await processor.ProcessDueCommandsAsync());

            Assert.Empty(_recorder.Handled);
            var stored = await store.GetAsync(id);
            Assert.Equal(ScheduledCommandStatus.Failed, stored.Status);
            Assert.Contains("attempts", stored.LastError);
        }

        [Fact]
        public async Task StoppingDuringDispatch_LeavesCommandClaimed_ForRedeliveryAfterLockExpiry()
        {
            using var provider = BuildProvider(o => o.LockDuration = TimeSpan.FromMinutes(1));
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();

            var id = await scheduler.ScheduleAsync(new CancellingCommand(), TimeSpan.Zero);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => processor.ProcessDueCommandsAsync(_recorder.Stop.Token));

            var stored = await store.GetAsync(id);
            Assert.Equal(ScheduledCommandStatus.Processing, stored.Status);
            Assert.Null(stored.LastError);
        }

        [Fact]
        public async Task StoppingDuringDispatch_DoesNotSpendAttemptsOfCommandsThatDidNotStart()
        {
            using var provider = BuildProvider(o =>
            {
                o.MaxAttempts = 1;
                o.LockDuration = TimeSpan.FromMinutes(1);
            });
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            var store = provider.GetRequiredService<IScheduledCommandStore>();

            await scheduler.ScheduleAsync(new CancellingCommand(), TimeSpan.Zero);
            var waitingId = await scheduler.ScheduleAsync(new SendReminderCommand { Email = "waiting" }, TimeSpan.FromSeconds(1));
            _clock.Advance(TimeSpan.FromSeconds(2));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => processor.ProcessDueCommandsAsync(_recorder.Stop.Token));

            var waiting = await store.GetAsync(waitingId);
            Assert.Equal(ScheduledCommandStatus.Pending, waiting.Status);
            Assert.Equal(0, waiting.Attempts);

            // After a restart the waiting command still runs.
            _clock.Advance(TimeSpan.FromMinutes(2));
            await processor.ProcessDueCommandsAsync();
            Assert.Equal(new[] { "waiting" }, _recorder.Handled);
            Assert.Equal(ScheduledCommandStatus.Completed, (await store.GetAsync(waitingId)).Status);
        }

        [Fact]
        public async Task SlowBatch_SecondProcessor_NeverRunsACommandTwice()
        {
            using var provider = BuildProvider(o => o.LockDuration = TimeSpan.FromMinutes(1));
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processorA = provider.GetRequiredService<ScheduledCommandProcessor>();
            var processorB = new ScheduledCommandProcessor(
                provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<IScheduledCommandStore>(),
                provider.GetRequiredService<Cortex.Mediator.Scheduling.Serialization.IScheduledCommandSerializer>(),
                new SchedulingOptions { LockDuration = TimeSpan.FromMinutes(1), BatchSize = 1 },
                _clock);
            _recorder.Clock = _clock;
            // Each command takes 40 s (less than the 1 min lock); a second processor runs one command while command 2 finishes.
            _recorder.AfterSecondSlowCommand = () => processorB.ProcessDueCommandsAsync();

            for (var i = 1; i <= 3; i++)
                await scheduler.ScheduleAsync(new SlowCommand { Name = i.ToString() }, TimeSpan.Zero);

            await processorA.ProcessDueCommandsAsync();

            Assert.Equal(new[] { "1", "2", "3" }, _recorder.Handled.OrderBy(n => n));
        }

        [Fact]
        public async Task HostedService_DispatchesDueCommands_UntilStopped()
        {
            using var provider = BuildProvider(o => o.PollingInterval = TimeSpan.FromMilliseconds(20));
            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            await scheduler.ScheduleAsync(new SendReminderCommand { Email = "hosted" }, TimeSpan.Zero);

            await processor.StartAsync(CancellationToken.None);
            try
            {
                var deadline = DateTime.UtcNow.AddSeconds(10);
                while (_recorder.Handled.IsEmpty && DateTime.UtcNow < deadline)
                    await Task.Delay(10);
            }
            finally
            {
                await processor.StopAsync(CancellationToken.None);
            }

            Assert.Equal(new[] { "hosted" }, _recorder.Handled);
        }
    }
}
