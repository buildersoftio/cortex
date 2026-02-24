using Cortex.Mediator;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Notification Publish Strategy Tests

    public class StrategyTestNotification : INotification
    {
        public string Message { get; set; } = string.Empty;
    }

    public class StrategyTestHandler1 : INotificationHandler<StrategyTestNotification>
    {
        private readonly List<string> _log;

        public StrategyTestHandler1(List<string> log)
        {
            _log = log;
        }

        public Task Handle(StrategyTestNotification notification, CancellationToken cancellationToken)
        {
            _log.Add($"Handler1: {notification.Message}");
            return Task.CompletedTask;
        }
    }

    public class StrategyTestHandler2 : INotificationHandler<StrategyTestNotification>
    {
        private readonly List<string> _log;

        public StrategyTestHandler2(List<string> log)
        {
            _log = log;
        }

        public Task Handle(StrategyTestNotification notification, CancellationToken cancellationToken)
        {
            _log.Add($"Handler2: {notification.Message}");
            return Task.CompletedTask;
        }
    }

    public class ThrowingStrategyHandler : INotificationHandler<StrategyTestNotification>
    {
        private readonly List<string> _log;

        public ThrowingStrategyHandler(List<string> log)
        {
            _log = log;
        }

        public Task Handle(StrategyTestNotification notification, CancellationToken cancellationToken)
        {
            _log.Add("ThrowingHandler: Before throw");
            throw new InvalidOperationException("Handler failed");
        }
    }

    #endregion

    public class NotificationPublishStrategyTests
    {
        #region MediatorOptions Tests

        [Fact]
        public void DefaultStrategy_ShouldBeParallel()
        {
            // Arrange & Act
            var options = new MediatorOptions();

            // Assert - verify default through DI registration
            var services = new ServiceCollection();
            services.AddCortexMediator(new Type[0], _ => { });
            var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(INotificationPublishStrategy));

            Assert.NotNull(descriptor);
            Assert.Equal(typeof(ParallelNotificationStrategy), descriptor!.ImplementationType);
        }

        [Fact]
        public void UseNotificationPublishStrategy_ShouldRegisterCustomStrategy()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddCortexMediator(new Type[0], options =>
            {
                options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();
            });

            // Assert
            var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(INotificationPublishStrategy));
            Assert.NotNull(descriptor);
            Assert.Equal(typeof(SequentialNotificationStrategy), descriptor!.ImplementationType);
        }

        [Fact]
        public void UseNotificationPublishStrategy_ShouldReturnOptionsForFluent()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act
            var result = options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();

            // Assert
            Assert.Same(options, result);
        }

        #endregion

        #region ParallelNotificationStrategy Tests

        [Fact]
        public async Task ParallelStrategy_ShouldExecuteAllHandlers()
        {
            // Arrange
            var log = new List<string>();
            var services = BuildServices(log);
            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new StrategyTestNotification { Message = "Parallel" });

            // Assert
            Assert.Contains("Handler1: Parallel", log);
            Assert.Contains("Handler2: Parallel", log);
        }

        [Fact]
        public async Task ParallelStrategy_WithThrowingHandler_ShouldPropagateException()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<INotificationPublishStrategy, ParallelNotificationStrategy>();
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new ThrowingStrategyHandler(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                mediator.PublishAsync(new StrategyTestNotification { Message = "Fail" }));
        }

        #endregion

        #region SequentialNotificationStrategy Tests

        [Fact]
        public async Task SequentialStrategy_ShouldExecuteHandlersInOrder()
        {
            // Arrange
            var log = new List<string>();
            var services = BuildServices(log, strategyType: typeof(SequentialNotificationStrategy));
            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new StrategyTestNotification { Message = "Sequential" });

            // Assert - sequential guarantees order matches registration order
            Assert.Equal(2, log.Count);
            Assert.Equal("Handler1: Sequential", log[0]);
            Assert.Equal("Handler2: Sequential", log[1]);
        }

        [Fact]
        public async Task SequentialStrategy_WithThrowingHandler_ShouldStopAndPropagate()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<INotificationPublishStrategy, SequentialNotificationStrategy>();
            // Throwing handler is first
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new ThrowingStrategyHandler(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                mediator.PublishAsync(new StrategyTestNotification { Message = "Fail" }));

            // Second handler should NOT have executed
            Assert.DoesNotContain("Handler1: Fail", log);
            Assert.Contains("ThrowingHandler: Before throw", log);
        }

        [Fact]
        public async Task SequentialStrategy_WithCancellation_ShouldStopBetweenHandlers()
        {
            // Arrange
            var log = new List<string>();
            var cts = new CancellationTokenSource();

            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<INotificationPublishStrategy, SequentialNotificationStrategy>();
            // First handler cancels the token
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new CancellingHandler(log, cts));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                mediator.PublishAsync(new StrategyTestNotification { Message = "Cancel" }, cts.Token));

            // First handler executed, second should not
            Assert.Contains("Cancelling: Cancel", log);
            Assert.DoesNotContain("Handler1: Cancel", log);
        }

        #endregion

        #region StopOnFirstFailureNotificationStrategy Tests

        [Fact]
        public async Task StopOnFirstFailureStrategy_ShouldExecuteAllWhenNoFailure()
        {
            // Arrange
            var log = new List<string>();
            var services = BuildServices(log, strategyType: typeof(StopOnFirstFailureNotificationStrategy));
            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new StrategyTestNotification { Message = "NoFail" });

            // Assert
            Assert.Equal(2, log.Count);
            Assert.Equal("Handler1: NoFail", log[0]);
            Assert.Equal("Handler2: NoFail", log[1]);
        }

        [Fact]
        public async Task StopOnFirstFailureStrategy_ShouldStopOnFirstException()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<INotificationPublishStrategy, StopOnFirstFailureNotificationStrategy>();
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new ThrowingStrategyHandler(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler2(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                mediator.PublishAsync(new StrategyTestNotification { Message = "StopOnFail" }));

            // First handler ran, throwing handler ran, third handler should NOT
            Assert.Contains("Handler1: StopOnFail", log);
            Assert.Contains("ThrowingHandler: Before throw", log);
            Assert.DoesNotContain("Handler2: StopOnFail", log);
        }

        #endregion

        #region Integration with AddCortexMediator

        [Fact]
        public async Task AddCortexMediator_WithSequentialStrategy_ShouldUseIt()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddCortexMediator(new Type[0], options =>
            {
                options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();
            });

            // Register handlers manually (not via assembly scanning)
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler2(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new StrategyTestNotification { Message = "DI" });

            // Assert - sequential preserves order
            Assert.Equal(2, log.Count);
            Assert.Equal("Handler1: DI", log[0]);
            Assert.Equal("Handler2: DI", log[1]);
        }

        [Fact]
        public async Task Mediator_WithoutStrategyRegistered_ShouldFallbackToParallel()
        {
            // Arrange - manual DI without AddCortexMediator, no strategy registered
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler2(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act - should not throw and should execute all handlers
            await mediator.PublishAsync(new StrategyTestNotification { Message = "Fallback" });

            // Assert
            Assert.Contains("Handler1: Fallback", log);
            Assert.Contains("Handler2: Fallback", log);
        }

        #endregion

        #region Custom Strategy Tests

        [Fact]
        public async Task CustomStrategy_ShouldBeUsable()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<INotificationPublishStrategy>(new ReverseOrderStrategy());
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler2(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new StrategyTestNotification { Message = "Reverse" });

            // Assert - custom strategy reverses the order
            Assert.Equal(2, log.Count);
            Assert.Equal("Handler2: Reverse", log[0]);
            Assert.Equal("Handler1: Reverse", log[1]);
        }

        #endregion

        #region Helpers

        private static ServiceCollection BuildServices(List<string> log, Type strategyType = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton(typeof(INotificationPublishStrategy),
                strategyType ?? typeof(ParallelNotificationStrategy));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler1(log));
            services.AddTransient<INotificationHandler<StrategyTestNotification>>(sp =>
                new StrategyTestHandler2(log));
            return services;
        }

        #endregion
    }

    #region Additional Test Handlers

    internal class CancellingHandler : INotificationHandler<StrategyTestNotification>
    {
        private readonly List<string> _log;
        private readonly CancellationTokenSource _cts;

        public CancellingHandler(List<string> log, CancellationTokenSource cts)
        {
            _log = log;
            _cts = cts;
        }

        public Task Handle(StrategyTestNotification notification, CancellationToken cancellationToken)
        {
            _log.Add($"Cancelling: {notification.Message}");
            _cts.Cancel();
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Custom strategy that executes handlers in reverse order — used to verify custom strategies work.
    /// </summary>
    internal class ReverseOrderStrategy : INotificationPublishStrategy
    {
        public async Task PublishAsync(
            IEnumerable<Func<Task>> handlerDelegates,
            CancellationToken cancellationToken)
        {
            var delegates = handlerDelegates.Reverse().ToList();
            foreach (var handler in delegates)
            {
                await handler();
            }
        }
    }

    #endregion
}
