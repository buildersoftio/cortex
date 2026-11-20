using Cortex.Mediator.Commands;
using Cortex.Mediator.Scheduling;
using Cortex.Mediator.Scheduling.Serialization;
using Cortex.Mediator.Scheduling.Stores;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public class CommandSchedulerTests
    {
        public class RetryPaymentCommand : ICommand
        {
            public string PaymentId { get; set; } = string.Empty;
        }

        private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        private readonly InMemoryScheduledCommandStore _store = new();
        private readonly JsonScheduledCommandSerializer _serializer = new();
        private readonly FakeTimeProvider _clock = new(T0);

        private CommandScheduler CreateScheduler() => new(_store, _serializer, _clock);

        [Fact]
        public async Task ScheduleAsync_WithDelay_StoresPendingCommandDueAfterDelay()
        {
            var scheduler = CreateScheduler();

            var id = await scheduler.ScheduleAsync(new RetryPaymentCommand { PaymentId = "p1" }, TimeSpan.FromMinutes(5));

            var stored = await _store.GetAsync(id);
            Assert.NotNull(stored);
            Assert.Equal(ScheduledCommandStatus.Pending, stored.Status);
            Assert.Equal(T0.AddMinutes(5), stored.ExecuteAt);
            Assert.Equal(T0, stored.CreatedAt);
            Assert.Equal(0, stored.Attempts);
            Assert.Equal(_serializer.GetTypeName(new RetryPaymentCommand()), stored.CommandType);
            var revived = Assert.IsType<RetryPaymentCommand>(_serializer.Deserialize(stored.CommandType, stored.Payload));
            Assert.Equal("p1", revived.PaymentId);
        }

        [Fact]
        public async Task ScheduleAsync_AtTime_StoresThatTime()
        {
            var scheduler = CreateScheduler();
            var at = new DateTimeOffset(2026, 1, 2, 9, 0, 0, TimeSpan.FromHours(1));

            var id = await scheduler.ScheduleAsync(new RetryPaymentCommand(), at);

            Assert.Equal(at, (await _store.GetAsync(id)).ExecuteAt);
        }

        [Fact]
        public async Task ScheduleAsync_ReturnsUniqueIds()
        {
            var scheduler = CreateScheduler();

            var a = await scheduler.ScheduleAsync(new RetryPaymentCommand(), TimeSpan.Zero);
            var b = await scheduler.ScheduleAsync(new RetryPaymentCommand(), TimeSpan.Zero);

            Assert.False(string.IsNullOrWhiteSpace(a));
            Assert.NotEqual(a, b);
        }

        [Fact]
        public async Task ScheduleAsync_NullCommand_Throws()
        {
            var scheduler = CreateScheduler();

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => scheduler.ScheduleAsync<RetryPaymentCommand>(null!, TimeSpan.Zero));
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => scheduler.ScheduleAsync<RetryPaymentCommand>(null!, T0));
        }

        [Fact]
        public async Task ScheduleAsync_NegativeDelay_Throws()
        {
            var scheduler = CreateScheduler();

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                () => scheduler.ScheduleAsync(new RetryPaymentCommand(), TimeSpan.FromSeconds(-1)));
        }

        [Fact]
        public async Task CancelAsync_PendingCommand_MarksItCancelled()
        {
            var scheduler = CreateScheduler();
            var id = await scheduler.ScheduleAsync(new RetryPaymentCommand(), TimeSpan.FromHours(1));

            await scheduler.CancelAsync(id);

            Assert.Equal(ScheduledCommandStatus.Cancelled, (await _store.GetAsync(id)).Status);
        }

        [Fact]
        public async Task CancelAsync_UnknownOrCompletedCommand_IsANoOp()
        {
            var scheduler = CreateScheduler();
            var id = await scheduler.ScheduleAsync(new RetryPaymentCommand(), TimeSpan.Zero);
            await _store.ClaimDueAsync(T0, 10, TimeSpan.FromMinutes(1));
            await _store.MarkCompletedAsync(id, 1);

            await scheduler.CancelAsync(id);
            await scheduler.CancelAsync("does-not-exist");

            Assert.Equal(ScheduledCommandStatus.Completed, (await _store.GetAsync(id)).Status);
        }

        [Fact]
        public async Task CancelAsync_EmptyId_Throws()
        {
            var scheduler = CreateScheduler();

            await Assert.ThrowsAsync<ArgumentException>(() => scheduler.CancelAsync(" "));
        }

        [Fact]
        public void Constructor_NullDependencies_Throw()
        {
            Assert.Throws<ArgumentNullException>(() => new CommandScheduler(null!, _serializer, _clock));
            Assert.Throws<ArgumentNullException>(() => new CommandScheduler(_store, null!, _clock));
            Assert.Throws<ArgumentNullException>(() => new CommandScheduler(_store, _serializer, null!));
        }
    }
}
