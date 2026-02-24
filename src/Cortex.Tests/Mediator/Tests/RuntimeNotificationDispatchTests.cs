using Cortex.Mediator;
using Cortex.Mediator.Notifications;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Runtime Notification Dispatch

    public class OrderCreatedNotification : INotification
    {
        public string OrderId { get; set; } = string.Empty;
    }

    public class OrderCreatedHandler : INotificationHandler<OrderCreatedNotification>
    {
        private readonly List<string> _log;

        public OrderCreatedHandler(List<string> log)
        {
            _log = log;
        }

        public Task Handle(OrderCreatedNotification notification, CancellationToken cancellationToken)
        {
            _log.Add($"OrderCreated: {notification.OrderId}");
            return Task.CompletedTask;
        }
    }

    public class PaymentProcessedNotification : INotification
    {
        public string PaymentId { get; set; } = string.Empty;
    }

    public class PaymentProcessedHandler : INotificationHandler<PaymentProcessedNotification>
    {
        private readonly List<string> _log;

        public PaymentProcessedHandler(List<string> log)
        {
            _log = log;
        }

        public Task Handle(PaymentProcessedNotification notification, CancellationToken cancellationToken)
        {
            _log.Add($"PaymentProcessed: {notification.PaymentId}");
            return Task.CompletedTask;
        }
    }

    public class RuntimeNotificationPipelineBehavior<TNotification> : INotificationPipelineBehavior<TNotification>
        where TNotification : INotification
    {
        private readonly List<string> _log;

        public RuntimeNotificationPipelineBehavior(List<string> log)
        {
            _log = log;
        }

        public async Task Handle(TNotification notification, NotificationHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add($"Before: {typeof(TNotification).Name}");
            await next();
            _log.Add($"After: {typeof(TNotification).Name}");
        }
    }

    #endregion

    public class RuntimeNotificationDispatchTests
    {
        [Fact]
        public async Task PublishAsync_NonGeneric_ShouldDispatchToCorrectHandler()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();

            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new OrderCreatedHandler(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            INotification notification = new OrderCreatedNotification { OrderId = "ORD-001" };

            // Act - use the non-generic overload
            await mediator.PublishAsync(notification);

            // Assert
            Assert.Single(log);
            Assert.Equal("OrderCreated: ORD-001", log[0]);
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_WithNullNotification_ShouldThrowArgumentNullException()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                mediator.PublishAsync((INotification)null!));
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_WithMultipleHandlers_ShouldDispatchToAll()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();

            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new OrderCreatedHandler(log));
            // Register a second handler for the same notification
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new OrderCreatedHandler(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            INotification notification = new OrderCreatedNotification { OrderId = "ORD-002" };

            // Act
            await mediator.PublishAsync(notification);

            // Assert
            Assert.Equal(2, log.Count);
            Assert.All(log, entry => Assert.Equal("OrderCreated: ORD-002", entry));
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_WithNoHandlers_ShouldCompleteSuccessfully()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            INotification notification = new OrderCreatedNotification { OrderId = "ORD-003" };

            // Act & Assert - should not throw
            await mediator.PublishAsync(notification);
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_WithPipelineBehavior_ShouldExecuteBehavior()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();

            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new OrderCreatedHandler(log));
            services.AddTransient<INotificationPipelineBehavior<OrderCreatedNotification>>(sp =>
                new RuntimeNotificationPipelineBehavior<OrderCreatedNotification>(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            INotification notification = new OrderCreatedNotification { OrderId = "ORD-004" };

            // Act
            await mediator.PublishAsync(notification);

            // Assert
            Assert.Equal(3, log.Count);
            Assert.Equal("Before: OrderCreatedNotification", log[0]);
            Assert.Equal("OrderCreated: ORD-004", log[1]);
            Assert.Equal("After: OrderCreatedNotification", log[2]);
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_ShouldPassCancellationToken()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            CancellationToken capturedToken = default;

            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new DelegateNotificationHandler<OrderCreatedNotification>((n, ct) =>
                {
                    capturedToken = ct;
                    return Task.CompletedTask;
                }));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            INotification notification = new OrderCreatedNotification { OrderId = "ORD-005" };

            // Act
            await mediator.PublishAsync(notification, cts.Token);

            // Assert
            Assert.Equal(cts.Token, capturedToken);
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_DomainEventsScenario_ShouldDispatchMultipleTypes()
        {
            // Arrange - simulates an aggregate root with mixed domain events
            var log = new List<string>();
            var services = new ServiceCollection();

            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                new OrderCreatedHandler(log));
            services.AddTransient<INotificationHandler<PaymentProcessedNotification>>(sp =>
                new PaymentProcessedHandler(log));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Simulate domain events list from an aggregate root
            var domainEvents = new List<INotification>
            {
                new OrderCreatedNotification { OrderId = "ORD-100" },
                new PaymentProcessedNotification { PaymentId = "PAY-200" },
                new OrderCreatedNotification { OrderId = "ORD-101" }
            };

            // Act - dispatch all events using the non-generic overload
            foreach (var domainEvent in domainEvents)
            {
                await mediator.PublishAsync(domainEvent);
            }

            // Assert
            Assert.Equal(3, log.Count);
            Assert.Equal("OrderCreated: ORD-100", log[0]);
            Assert.Equal("PaymentProcessed: PAY-200", log[1]);
            Assert.Equal("OrderCreated: ORD-101", log[2]);
        }

        [Fact]
        public async Task PublishAsync_NonGeneric_ShouldProduceSameResultAsGenericOverload()
        {
            // Arrange
            var genericLog = new List<string>();
            var runtimeLog = new List<string>();

            // Build two identical service providers
            ServiceProvider BuildProvider(List<string> log)
            {
                var services = new ServiceCollection();
                services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
                services.AddTransient<INotificationHandler<OrderCreatedNotification>>(sp =>
                    new OrderCreatedHandler(log));
                services.AddTransient<INotificationPipelineBehavior<OrderCreatedNotification>>(sp =>
                    new RuntimeNotificationPipelineBehavior<OrderCreatedNotification>(log));
                return services.BuildServiceProvider();
            }

            var genericProvider = BuildProvider(genericLog);
            var runtimeProvider = BuildProvider(runtimeLog);

            var notification = new OrderCreatedNotification { OrderId = "ORD-COMPARE" };

            // Act - generic overload
            var genericMediator = genericProvider.GetRequiredService<IMediator>();
            await genericMediator.PublishAsync(notification);

            // Act - non-generic overload
            var runtimeMediator = runtimeProvider.GetRequiredService<IMediator>();
            await runtimeMediator.PublishAsync((INotification)notification);

            // Assert - both should produce identical logs
            Assert.Equal(genericLog.Count, runtimeLog.Count);
            for (int i = 0; i < genericLog.Count; i++)
            {
                Assert.Equal(genericLog[i], runtimeLog[i]);
            }
        }
    }

    /// <summary>
    /// Helper handler that delegates to a provided function, useful for capturing arguments in tests.
    /// </summary>
    internal class DelegateNotificationHandler<TNotification> : INotificationHandler<TNotification>
        where TNotification : INotification
    {
        private readonly Func<TNotification, CancellationToken, Task> _handler;

        public DelegateNotificationHandler(Func<TNotification, CancellationToken, Task> handler)
        {
            _handler = handler;
        }

        public Task Handle(TNotification notification, CancellationToken cancellationToken)
        {
            return _handler(notification, cancellationToken);
        }
    }
}
