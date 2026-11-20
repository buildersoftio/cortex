using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Scheduling;
using Cortex.Mediator.Scheduling.DependencyInjection;
using Cortex.Mediator.Scheduling.Serialization;
using Cortex.Mediator.Scheduling.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public class SchedulingServiceCollectionTests
    {
        private static ServiceCollection NewServices()
        {
            var services = new ServiceCollection();
            services.AddCortexMediator(Array.Empty<Type>());
            return services;
        }

        [Fact]
        public void AddCortexMediatorScheduling_RegistersDefaults()
        {
            var services = NewServices();

            services.AddCortexMediatorScheduling();
            using var provider = services.BuildServiceProvider();

            Assert.IsType<CommandScheduler>(provider.GetRequiredService<ICommandScheduler>());
            Assert.IsType<InMemoryScheduledCommandStore>(provider.GetRequiredService<IScheduledCommandStore>());
            Assert.IsType<JsonScheduledCommandSerializer>(provider.GetRequiredService<IScheduledCommandSerializer>());
            Assert.Same(TimeProvider.System, provider.GetRequiredService<TimeProvider>());
            var hosted = Assert.Single(provider.GetServices<IHostedService>());
            Assert.Same(provider.GetRequiredService<ScheduledCommandProcessor>(), hosted);
        }

        [Fact]
        public void AddCortexMediatorScheduling_AppliesOptions()
        {
            var services = NewServices();

            services.AddCortexMediatorScheduling(o =>
            {
                o.PollingInterval = TimeSpan.FromSeconds(1);
                o.BatchSize = 7;
            });
            using var provider = services.BuildServiceProvider();

            var options = provider.GetRequiredService<SchedulingOptions>();
            Assert.Equal(TimeSpan.FromSeconds(1), options.PollingInterval);
            Assert.Equal(7, options.BatchSize);
        }

        [Fact]
        public void EnableProcessorFalse_RegistersNoHostedService_ButKeepsTheProcessor()
        {
            var services = NewServices();

            services.AddCortexMediatorScheduling(o => o.EnableProcessor = false);
            using var provider = services.BuildServiceProvider();

            Assert.Empty(provider.GetServices<IHostedService>());
            Assert.NotNull(provider.GetRequiredService<ScheduledCommandProcessor>());
        }

        [Fact]
        public void CallingTwice_RegistersOneHostedService()
        {
            var services = NewServices();

            services.AddCortexMediatorScheduling();
            services.AddCortexMediatorScheduling();
            using var provider = services.BuildServiceProvider();

            Assert.Single(provider.GetServices<IHostedService>());
        }

        [Fact]
        public void CustomStoreRegisteredFirst_IsKept()
        {
            var services = NewServices();
            var store = new InMemoryScheduledCommandStore();
            services.AddSingleton<IScheduledCommandStore>(store);

            services.AddCortexMediatorScheduling();
            using var provider = services.BuildServiceProvider();

            Assert.Same(store, provider.GetRequiredService<IScheduledCommandStore>());
        }

        [Theory]
        [InlineData(nameof(SchedulingOptions.PollingInterval))]
        [InlineData(nameof(SchedulingOptions.BatchSize))]
        [InlineData(nameof(SchedulingOptions.MaxAttempts))]
        [InlineData(nameof(SchedulingOptions.RetryDelay))]
        [InlineData(nameof(SchedulingOptions.LockDuration))]
        public void InvalidOptions_Throw(string property)
        {
            var services = NewServices();

            var ex = Assert.Throws<ArgumentOutOfRangeException>(() => services.AddCortexMediatorScheduling(o =>
            {
                switch (property)
                {
                    case nameof(SchedulingOptions.PollingInterval): o.PollingInterval = TimeSpan.Zero; break;
                    case nameof(SchedulingOptions.BatchSize): o.BatchSize = 0; break;
                    case nameof(SchedulingOptions.MaxAttempts): o.MaxAttempts = 0; break;
                    case nameof(SchedulingOptions.RetryDelay): o.RetryDelay = TimeSpan.FromSeconds(-1); break;
                    case nameof(SchedulingOptions.LockDuration): o.LockDuration = TimeSpan.Zero; break;
                }
            }));

            Assert.Equal(property, ex.ParamName);
        }

        [Fact]
        public void NullServices_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddCortexMediatorScheduling());
        }

        [Fact]
        public void AddDbScheduledCommandStore_BeforeAddCortexMediatorScheduling_StillReplacesTheInMemoryStore()
        {
            var services = NewServices();

            services.AddDbScheduledCommandStore(_ => throw new InvalidOperationException("not opened"), new SqliteDialect());
            services.AddCortexMediatorScheduling();
            using var provider = services.BuildServiceProvider();

            Assert.IsType<DbScheduledCommandStore>(provider.GetRequiredService<IScheduledCommandStore>());
            Assert.Single(services, d => d.ServiceType == typeof(IScheduledCommandStore));
        }
    }
}
