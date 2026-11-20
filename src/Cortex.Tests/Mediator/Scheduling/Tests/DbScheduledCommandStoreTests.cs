using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Scheduling;
using Cortex.Mediator.Scheduling.DependencyInjection;
using Cortex.Mediator.Scheduling.Stores;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public sealed class DbScheduledCommandStoreTests : IDisposable
    {
        public class ChargeCardCommand : ICommand
        {
            public string CardId { get; set; } = string.Empty;
        }

        public sealed class ChargeLog
        {
            public ConcurrentQueue<string> Charged { get; } = new();
        }

        public class ChargeCardHandler : ICommandHandler<ChargeCardCommand>
        {
            private readonly ChargeLog _log;
            public ChargeCardHandler(ChargeLog log) => _log = log;

            public Task Handle(ChargeCardCommand command, CancellationToken cancellationToken)
            {
                _log.Charged.Enqueue(command.CardId);
                return Task.CompletedTask;
            }
        }

        private static readonly DateTimeOffset T0 = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan Lock = TimeSpan.FromMinutes(5);

        private readonly string _path = Path.Combine(Path.GetTempPath(), $"cortex-scheduling-{Guid.NewGuid():N}.db");
        private string ConnectionString => $"Data Source={_path}";

        public void Dispose()
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(_path))
                File.Delete(_path);
        }

        private DbScheduledCommandStore NewStore(Action<DbScheduledCommandStoreOptions>? configure = null)
        {
            var options = new DbScheduledCommandStoreOptions();
            configure?.Invoke(options);
            return new DbScheduledCommandStore(() => new SqliteConnection(ConnectionString), new SqliteDialect(), options);
        }

        private static ScheduledCommand Pending(string id, DateTimeOffset executeAt) => new()
        {
            Id = id,
            CommandType = "Some.Command, Some.Assembly",
            Payload = "{\"x\":1}",
            ExecuteAt = executeAt,
            CreatedAt = T0,
            Status = ScheduledCommandStatus.Pending
        };

        [Fact]
        public async Task Add_CreatesTheTable_AndPersistsEveryField_AcrossStoreInstances()
        {
            var executeAt = new DateTimeOffset(2026, 1, 2, 9, 30, 0, TimeSpan.FromHours(2));
            await NewStore().AddAsync(Pending("a", executeAt));

            var stored = await NewStore().GetAsync("a");

            Assert.NotNull(stored);
            Assert.Equal("a", stored.Id);
            Assert.Equal("Some.Command, Some.Assembly", stored.CommandType);
            Assert.Equal("{\"x\":1}", stored.Payload);
            Assert.Equal(executeAt, stored.ExecuteAt);
            Assert.Equal(T0, stored.CreatedAt);
            Assert.Equal(ScheduledCommandStatus.Pending, stored.Status);
            Assert.Equal(0, stored.Attempts);
            Assert.Null(stored.LastError);
            Assert.Null(stored.LockedUntil);
            Assert.Null(await NewStore().GetAsync("missing"));
        }

        [Fact]
        public async Task EnsureTable_IsIdempotent_AndHonoursCustomNames()
        {
            var store = NewStore(o => o.TableName = "MyJobs");

            await store.EnsureTableAsync();
            await store.EnsureTableAsync();
            await NewStore(o => o.TableName = "MyJobs").EnsureTableAsync();

            using var connection = new SqliteConnection(ConnectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'MyJobs'";
            Assert.Equal(1L, (long)command.ExecuteScalar()!);
        }

        [Fact]
        public async Task ClaimDue_ReturnsDueCommandsInOrder_LocksThem_AndCountsAttempts()
        {
            var store = NewStore();
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
            Assert.Empty(await store.ClaimDueAsync(T0.AddMinutes(4), 10, Lock));
        }

        [Fact]
        public async Task ClaimDue_RespectsMaxCount_AndReoffersExpiredLocks()
        {
            var store = NewStore();
            for (var i = 0; i < 3; i++)
                await store.AddAsync(Pending("c" + i, T0.AddSeconds(i)));

            var first = await store.ClaimDueAsync(T0.AddMinutes(1), 2, Lock);
            var reclaimed = await store.ClaimDueAsync(T0.AddMinutes(1) + Lock, 10, Lock);

            Assert.Equal(new[] { "c0", "c1" }, first.Select(c => c.Id));
            Assert.Equal(new[] { "c0", "c1", "c2" }, reclaimed.Select(c => c.Id));
            Assert.Equal(new[] { 2, 2, 1 }, reclaimed.Select(c => c.Attempts));
        }

        [Fact]
        public async Task ClaimDue_FromTwoStoresConcurrently_NeverReturnsTheSameCommandTwice()
        {
            var seed = NewStore();
            for (var i = 0; i < 60; i++)
                await seed.AddAsync(Pending("c" + i.ToString("D2"), T0));

            var stores = new[] { NewStore(), NewStore(), NewStore() };
            var claimedIds = new ConcurrentBag<string>();
            await Task.WhenAll(stores.Select(store => Task.Run(async () =>
            {
                IReadOnlyList<ScheduledCommand> batch;
                do
                {
                    batch = await store.ClaimDueAsync(T0, 7, Lock);
                    foreach (var c in batch)
                        claimedIds.Add(c.Id);
                }
                while (batch.Count > 0);
            })));

            Assert.Equal(60, claimedIds.Count);
            Assert.Equal(60, claimedIds.Distinct().Count());
        }

        [Fact]
        public async Task Cancel_OnlyAffectsPendingCommands()
        {
            var store = NewStore();
            await store.AddAsync(Pending("pending", T0.AddHours(1)));
            await store.AddAsync(Pending("running", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            Assert.True(await store.CancelAsync("pending"));
            Assert.False(await store.CancelAsync("pending"));
            Assert.False(await store.CancelAsync("running"));
            Assert.False(await store.CancelAsync("missing"));

            Assert.Equal(ScheduledCommandStatus.Cancelled, (await store.GetAsync("pending")).Status);
            var later = await store.ClaimDueAsync(T0.AddDays(1), 10, Lock);
            Assert.Equal(new[] { "running" }, later.Select(c => c.Id));
        }

        [Fact]
        public async Task MarkCompleted_And_MarkFailed_PersistState()
        {
            var store = NewStore();
            await store.AddAsync(Pending("done", T0));
            await store.AddAsync(Pending("retry", T0));
            await store.AddAsync(Pending("dead", T0));
            await store.ClaimDueAsync(T0, 10, Lock);

            await store.MarkCompletedAsync("done");
            await store.MarkFailedAsync("retry", "timeout", T0.AddMinutes(1));
            await store.MarkFailedAsync("dead", "bad card", null);

            var reader = NewStore();
            var done = await reader.GetAsync("done");
            var retry = await reader.GetAsync("retry");
            var dead = await reader.GetAsync("dead");
            Assert.Equal(ScheduledCommandStatus.Completed, done.Status);
            Assert.Null(done.LockedUntil);
            Assert.Equal(ScheduledCommandStatus.Pending, retry.Status);
            Assert.Equal(T0.AddMinutes(1), retry.ExecuteAt);
            Assert.Equal("timeout", retry.LastError);
            Assert.Null(retry.LockedUntil);
            Assert.Equal(ScheduledCommandStatus.Failed, dead.Status);
            Assert.Equal("bad card", dead.LastError);

            var next = await reader.ClaimDueAsync(T0.AddMinutes(1), 10, Lock);
            Assert.Equal(new[] { "retry" }, next.Select(c => c.Id));
        }

        [Theory]
        [InlineData("Scheduled Commands")]
        [InlineData("x; DROP TABLE y")]
        [InlineData("\"quoted\"")]
        [InlineData("")]
        [InlineData("1table")]
        public void InvalidTableName_Throws(string tableName)
        {
            Assert.Throws<ArgumentException>(() => NewStore(o => o.TableName = tableName));
        }

        [Fact]
        public void InvalidSchemaName_Throws()
        {
            Assert.Throws<ArgumentException>(() => NewStore(o => o.Schema = "dbo].[x"));
        }

        [Fact]
        public void Constructor_NullArguments_Throw()
        {
            Assert.Throws<ArgumentNullException>(() => new DbScheduledCommandStore(null!, new SqliteDialect()));
            Assert.Throws<ArgumentNullException>(() => new DbScheduledCommandStore(() => new SqliteConnection(ConnectionString), null!));
        }

        [Fact]
        public void Dialects_QuoteIdentifiers_AndLimitTheDueQuery()
        {
            Assert.Equal("[Jobs]", new SqlServerDialect().QuoteIdentifier("Jobs"));
            Assert.Equal("\"Jobs\"", new PostgreSqlDialect().QuoteIdentifier("Jobs"));
            Assert.Equal("\"Jobs\"", new SqliteDialect().QuoteIdentifier("Jobs"));

            Assert.Contains("TOP (@limit)", new SqlServerDialect().GetSelectDueIdsSql("[dbo].[Jobs]"));
            Assert.Contains("LIMIT @limit", new PostgreSqlDialect().GetSelectDueIdsSql("\"public\".\"Jobs\""));
            Assert.Contains("OBJECT_ID(N'[dbo].[Jobs]', N'U')", new SqlServerDialect().GetCreateTableSql("dbo", "Jobs"));
            Assert.Contains("CREATE TABLE IF NOT EXISTS \"public\".\"Jobs\"", new PostgreSqlDialect().GetCreateTableSql("public", "Jobs"));
        }

        [Fact]
        public async Task EndToEnd_ScheduledCommandIsDispatchedOnceThroughTheDatabaseStore()
        {
            var clock = new FakeTimeProvider(T0);
            var log = new ChargeLog();
            var services = new ServiceCollection();
            services.AddCortexMediator(Array.Empty<Type>());
            services.AddSingleton(log);
            services.AddScoped<ICommandHandler<ChargeCardCommand>, ChargeCardHandler>();
            services.AddSingleton<TimeProvider>(clock);
            services.AddCortexMediatorScheduling(o => o.EnableProcessor = false);
            services.AddDbScheduledCommandStore(_ => new SqliteConnection(ConnectionString), new SqliteDialect());
            using var provider = services.BuildServiceProvider();

            var scheduler = provider.GetRequiredService<ICommandScheduler>();
            var processor = provider.GetRequiredService<ScheduledCommandProcessor>();
            Assert.IsType<DbScheduledCommandStore>(provider.GetRequiredService<IScheduledCommandStore>());

            var id = await scheduler.ScheduleAsync(new ChargeCardCommand { CardId = "card-1" }, TimeSpan.FromMinutes(5));
            Assert.Equal(0, await processor.ProcessDueCommandsAsync());

            clock.Advance(TimeSpan.FromMinutes(5));
            Assert.Equal(1, await processor.ProcessDueCommandsAsync());
            Assert.Equal(0, await processor.ProcessDueCommandsAsync());

            Assert.Equal(new[] { "card-1" }, log.Charged);
            Assert.Equal(ScheduledCommandStatus.Completed, (await NewStore().GetAsync(id)).Status);
        }

        [Fact]
        public void AddDbScheduledCommandStore_InvalidTableName_ThrowsAtRegistration()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(() => services.AddDbScheduledCommandStore(
                _ => new SqliteConnection(ConnectionString), new SqliteDialect(), o => o.TableName = "bad name"));
        }
    }
}
