using Cortex.Mediator.Scheduling;
using Cortex.Mediator.Scheduling.Stores;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public class InMemoryScheduledCommandStoreTests
    {
        private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan Lock = TimeSpan.FromMinutes(5);

        private static ScheduledCommand Pending(string id, DateTimeOffset executeAt) => new()
        {
            Id = id,
            CommandType = "T, A",
            Payload = "{}",
            ExecuteAt = executeAt,
            CreatedAt = T0,
            Status = ScheduledCommandStatus.Pending
        };

        [Fact]
        public async Task ClaimDue_ReturnsOnlyDueCommands_InExecutionOrder()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("late", T0.AddMinutes(2)));
            await store.AddAsync(Pending("early", T0.AddMinutes(1)));
            await store.AddAsync(Pending("future", T0.AddHours(1)));

            var claimed = await store.ClaimDueAsync(T0.AddMinutes(3), 10, Lock);

            Assert.Equal(new[] { "early", "late" }, claimed.Select(c => c.Id));
            Assert.All(claimed, c =>
            {
                Assert.Equal(ScheduledCommandStatus.Processing, c.Status);
                Assert.Equal(1, c.Attempts);
                Assert.Equal(T0.AddMinutes(3) + Lock, c.LockedUntil);
            });
        }

        [Fact]
        public async Task ClaimDue_DoesNotReturnTheSameCommandTwice_WhileLocked()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));

            var first = await store.ClaimDueAsync(T0, 10, Lock);
            var second = await store.ClaimDueAsync(T0.AddMinutes(1), 10, Lock);

            Assert.Single(first);
            Assert.Empty(second);
        }

        [Fact]
        public async Task ClaimDue_ReoffersCommand_WhenLockExpired()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            var reclaimed = await store.ClaimDueAsync(T0 + Lock + TimeSpan.FromSeconds(1), 10, Lock);

            var command = Assert.Single(reclaimed);
            Assert.Equal(2, command.Attempts);
        }

        [Fact]
        public async Task ClaimDue_RespectsMaxCount()
        {
            var store = new InMemoryScheduledCommandStore();
            for (var i = 0; i < 5; i++)
                await store.AddAsync(Pending("c" + i, T0.AddSeconds(i)));

            var claimed = await store.ClaimDueAsync(T0.AddMinutes(1), 2, Lock);

            Assert.Equal(new[] { "c0", "c1" }, claimed.Select(c => c.Id));
        }

        [Fact]
        public async Task ClaimDue_ConcurrentCallers_NeverClaimTheSameCommand()
        {
            var store = new InMemoryScheduledCommandStore();
            for (var i = 0; i < 200; i++)
                await store.AddAsync(Pending("c" + i, T0));

            var results = await Task.WhenAll(Enumerable.Range(0, 8)
                .Select(_ => Task.Run(() => store.ClaimDueAsync(T0, 50, Lock))));

            var ids = results.SelectMany(r => r).Select(c => c.Id).ToList();
            Assert.Equal(200, ids.Count);
            Assert.Equal(ids.Count, ids.Distinct().Count());
        }

        [Fact]
        public async Task Cancel_PendingCommand_ReturnsTrue_AndIsNeverClaimed()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));

            var cancelled = await store.CancelAsync("a");
            var claimed = await store.ClaimDueAsync(T0.AddDays(1), 10, Lock);

            Assert.True(cancelled);
            Assert.Empty(claimed);
            Assert.Equal(ScheduledCommandStatus.Cancelled, (await store.GetAsync("a")).Status);
        }

        [Fact]
        public async Task Cancel_UnknownOrProcessingCommand_ReturnsFalse()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            Assert.False(await store.CancelAsync("missing"));
            Assert.False(await store.CancelAsync("a"));
            Assert.Equal(ScheduledCommandStatus.Processing, (await store.GetAsync("a")).Status);
        }

        [Fact]
        public async Task MarkCompleted_SetsStatus_AndClearsLock()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            await store.MarkCompletedAsync("a");

            var stored = await store.GetAsync("a");
            Assert.Equal(ScheduledCommandStatus.Completed, stored.Status);
            Assert.Null(stored.LockedUntil);
            Assert.Empty(await store.ClaimDueAsync(T0.AddDays(1), 10, Lock));
        }

        [Fact]
        public async Task MarkFailed_WithRetry_MakesCommandPendingAgainAtRetryTime()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            await store.MarkFailedAsync("a", "boom", T0.AddMinutes(1));

            var stored = await store.GetAsync("a");
            Assert.Equal(ScheduledCommandStatus.Pending, stored.Status);
            Assert.Equal("boom", stored.LastError);
            Assert.Equal(T0.AddMinutes(1), stored.ExecuteAt);
            Assert.Empty(await store.ClaimDueAsync(T0.AddSeconds(30), 10, Lock));
            Assert.Single(await store.ClaimDueAsync(T0.AddMinutes(1), 10, Lock));
        }

        [Fact]
        public async Task MarkFailed_WithoutRetry_MarksCommandFailed()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            await store.MarkFailedAsync("a", "boom", null);

            var stored = await store.GetAsync("a");
            Assert.Equal(ScheduledCommandStatus.Failed, stored.Status);
            Assert.Equal("boom", stored.LastError);
            Assert.Empty(await store.ClaimDueAsync(T0.AddDays(1), 10, Lock));
        }

        [Fact]
        public async Task Add_DuplicateId_Throws()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));

            await Assert.ThrowsAsync<InvalidOperationException>(() => store.AddAsync(Pending("a", T0)));
        }

        [Fact]
        public async Task Get_ReturnsACopy_SoCallersCannotMutateStoredState()
        {
            var store = new InMemoryScheduledCommandStore();
            await store.AddAsync(Pending("a", T0));

            var copy = await store.GetAsync("a");
            copy.Status = ScheduledCommandStatus.Completed;

            Assert.Equal(ScheduledCommandStatus.Pending, (await store.GetAsync("a")).Status);
            Assert.Null(await store.GetAsync("missing"));
        }
    }
}
