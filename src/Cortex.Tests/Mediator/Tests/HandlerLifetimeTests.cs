using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Handler Lifetime Tests

    // These types live in this assembly so Scrutor can discover them via the marker type.

    public class LifetimeTestCommand : ICommand<string>
    {
        public string Input { get; set; } = string.Empty;
    }

    public class LifetimeTestCommandHandler : ICommandHandler<LifetimeTestCommand, string>
    {
        public Task<string> Handle(LifetimeTestCommand command, CancellationToken cancellationToken)
        {
            return Task.FromResult(command.Input);
        }
    }

    public class LifetimeTestVoidCommand : ICommand { }

    public class LifetimeTestVoidCommandHandler : ICommandHandler<LifetimeTestVoidCommand>
    {
        public Task Handle(LifetimeTestVoidCommand command, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public class LifetimeTestQuery : IQuery<string>
    {
        public string Input { get; set; } = string.Empty;
    }

    public class LifetimeTestQueryHandler : IQueryHandler<LifetimeTestQuery, string>
    {
        public Task<string> Handle(LifetimeTestQuery query, CancellationToken cancellationToken)
        {
            return Task.FromResult(query.Input);
        }
    }

    public class LifetimeTestNotification : INotification
    {
        public string Data { get; set; } = string.Empty;
    }

    public class LifetimeTestNotificationHandler : INotificationHandler<LifetimeTestNotification>
    {
        public Task Handle(LifetimeTestNotification notification, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public class LifetimeTestStreamQuery : IStreamQuery<string> { }

    public class LifetimeTestStreamQueryHandler : IStreamQueryHandler<LifetimeTestStreamQuery, string>
    {
        public async IAsyncEnumerable<string> Handle(LifetimeTestStreamQuery query, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return "item";
            await Task.CompletedTask;
        }
    }

    #endregion

    public class HandlerLifetimeTests
    {
        private static readonly Type MarkerType = typeof(LifetimeTestCommandHandler);

        [Fact]
        public void DefaultHandlerLifetime_ShouldBeScoped()
        {
            // Arrange
            var options = new MediatorOptions();

            // Assert
            Assert.Equal(ServiceLifetime.Scoped, options.HandlerLifetime);
        }

        [Fact]
        public void DefaultRegistration_ShouldRegisterHandlersAsScoped()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act - default (no HandlerLifetime set)
            services.AddCortexMediator(new[] { MarkerType });

            // Assert
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestCommand, string>), ServiceLifetime.Scoped);
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestVoidCommand>), ServiceLifetime.Scoped);
            AssertHandlerLifetime(services, typeof(IQueryHandler<LifetimeTestQuery, string>), ServiceLifetime.Scoped);
            AssertHandlerLifetime(services, typeof(INotificationHandler<LifetimeTestNotification>), ServiceLifetime.Scoped);
            AssertHandlerLifetime(services, typeof(IStreamQueryHandler<LifetimeTestStreamQuery, string>), ServiceLifetime.Scoped);
        }

        [Fact]
        public void TransientLifetime_ShouldRegisterHandlersAsTransient()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Transient);

            // Assert
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestCommand, string>), ServiceLifetime.Transient);
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestVoidCommand>), ServiceLifetime.Transient);
            AssertHandlerLifetime(services, typeof(IQueryHandler<LifetimeTestQuery, string>), ServiceLifetime.Transient);
            AssertHandlerLifetime(services, typeof(INotificationHandler<LifetimeTestNotification>), ServiceLifetime.Transient);
            AssertHandlerLifetime(services, typeof(IStreamQueryHandler<LifetimeTestStreamQuery, string>), ServiceLifetime.Transient);
        }

        [Fact]
        public void SingletonLifetime_ShouldRegisterHandlersAsSingleton()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Singleton);

            // Assert
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestCommand, string>), ServiceLifetime.Singleton);
            AssertHandlerLifetime(services, typeof(ICommandHandler<LifetimeTestVoidCommand>), ServiceLifetime.Singleton);
            AssertHandlerLifetime(services, typeof(IQueryHandler<LifetimeTestQuery, string>), ServiceLifetime.Singleton);
            AssertHandlerLifetime(services, typeof(INotificationHandler<LifetimeTestNotification>), ServiceLifetime.Singleton);
            AssertHandlerLifetime(services, typeof(IStreamQueryHandler<LifetimeTestStreamQuery, string>), ServiceLifetime.Singleton);
        }

        [Fact]
        public void TransientHandlers_ShouldResolveNewInstanceEachTime()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Transient);

            var provider = services.BuildServiceProvider();

            // Act
            var handler1 = provider.GetRequiredService<ICommandHandler<LifetimeTestCommand, string>>();
            var handler2 = provider.GetRequiredService<ICommandHandler<LifetimeTestCommand, string>>();

            // Assert
            Assert.NotSame(handler1, handler2);
        }

        [Fact]
        public void SingletonHandlers_ShouldResolveSameInstance()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Singleton);

            var provider = services.BuildServiceProvider();

            // Act
            var handler1 = provider.GetRequiredService<ICommandHandler<LifetimeTestCommand, string>>();
            var handler2 = provider.GetRequiredService<ICommandHandler<LifetimeTestCommand, string>>();

            // Assert
            Assert.Same(handler1, handler2);
        }

        [Fact]
        public async Task TransientHandlers_ShouldWorkEndToEnd()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Transient);

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert - should dispatch correctly with transient handlers
            var result = await mediator.SendAsync(new LifetimeTestCommand { Input = "transient-test" });
            Assert.Equal("transient-test", result);

            var queryResult = await mediator.QueryAsync(new LifetimeTestQuery { Input = "query-test" });
            Assert.Equal("query-test", queryResult);
        }

        [Fact]
        public void HandlerLifetime_ShouldNotAffectMediatorLifetime()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddCortexMediator(
                new[] { MarkerType },
                options => options.HandlerLifetime = ServiceLifetime.Transient);

            // Assert - IMediator should remain Scoped regardless of handler lifetime
            var mediatorDescriptor = services.First(d => d.ServiceType == typeof(IMediator));
            Assert.Equal(ServiceLifetime.Scoped, mediatorDescriptor.Lifetime);
        }

        private static void AssertHandlerLifetime(IServiceCollection services, Type serviceType, ServiceLifetime expectedLifetime)
        {
            var descriptor = services.FirstOrDefault(d => d.ServiceType == serviceType);
            Assert.NotNull(descriptor);
            Assert.Equal(expectedLifetime, descriptor!.Lifetime);
        }
    }
}
