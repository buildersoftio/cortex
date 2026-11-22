using Cortex.Mediator;
using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cortex.Tests.Mediator.Tests
{
    public class ExceptionHandlingIntegrationTests
    {
        #region Test Commands, Queries, and Notifications

        public class FailingCommand : ICommand<string>
        {
            public bool ShouldFail { get; set; } = true;
        }

        public class FailingCommandHandler : ICommandHandler<FailingCommand, string>
        {
            public Task<string> Handle(FailingCommand command, CancellationToken cancellationToken)
            {
                if (command.ShouldFail)
                    throw new InvalidOperationException("Command failed");
                return Task.FromResult("Success");
            }
        }

        public class FailingVoidCommand : ICommand
        {
            public bool ShouldFail { get; set; } = true;
        }

        public class FailingVoidCommandHandler : ICommandHandler<FailingVoidCommand>
        {
            public Task Handle(FailingVoidCommand command, CancellationToken cancellationToken)
            {
                if (command.ShouldFail)
                    throw new InvalidOperationException("Void command failed");
                return Task.CompletedTask;
            }
        }

        public class FailingQuery : IQuery<string>
        {
            public bool ShouldFail { get; set; } = true;
        }

        public class FailingQueryHandler : IQueryHandler<FailingQuery, string>
        {
            public Task<string> Handle(FailingQuery query, CancellationToken cancellationToken)
            {
                if (query.ShouldFail)
                    throw new InvalidOperationException("Query failed");
                return Task.FromResult("Success");
            }
        }

        public class FailingNotification : INotification
        {
            public bool ShouldFail { get; set; } = true;
        }

        public class FailingNotificationHandler : INotificationHandler<FailingNotification>
        {
            public Task Handle(FailingNotification notification, CancellationToken cancellationToken)
            {
                if (notification.ShouldFail)
                    throw new InvalidOperationException("Notification failed");
                return Task.CompletedTask;
            }
        }

        public class NotificationRunLog
        {
            public List<string> Entries { get; } = new();
        }

        public class SecondNotificationHandler : INotificationHandler<FailingNotification>
        {
            private readonly NotificationRunLog _log;

            public SecondNotificationHandler(NotificationRunLog log) => _log = log;

            public Task Handle(FailingNotification notification, CancellationToken cancellationToken)
            {
                _log.Entries.Add("second");
                return Task.CompletedTask;
            }
        }

        public class TestExceptionHandler : IExceptionHandler
        {
            public bool ShouldHandle { get; set; } = true;
            public List<Exception> HandledExceptions { get; } = new();

            public Task<bool> HandleAsync(Exception exception, Type requestType, object request, CancellationToken cancellationToken)
            {
                HandledExceptions.Add(exception);
                return Task.FromResult(ShouldHandle);
            }
        }

        public class TestExceptionHandlerWithResult : IExceptionHandler<string>
        {
            public bool ShouldHandle { get; set; } = true;
            public string FallbackResult { get; set; } = "Fallback";
            public List<Exception> HandledExceptions { get; } = new();

            public Task<bool> HandleAsync(Exception exception, Type requestType, object request, CancellationToken cancellationToken)
            {
                HandledExceptions.Add(exception);
                return Task.FromResult(ShouldHandle);
            }

            public Task<(bool handled, string? result)> HandleWithResultAsync(Exception exception, Type requestType, object request, CancellationToken cancellationToken)
            {
                HandledExceptions.Add(exception);
                return Task.FromResult((ShouldHandle, ShouldHandle ? FallbackResult : null));
            }
        }

        #endregion

        private IServiceProvider CreateServiceProvider(Action<IServiceCollection>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

            // Use empty array to avoid auto-scanning this assembly for handlers
            services.AddCortexMediator(
                Array.Empty<Type>(),
                options => options.AddExceptionHandlingBehaviors());

            // Manually register handlers
            services.AddTransient<ICommandHandler<FailingCommand, string>, FailingCommandHandler>();
            services.AddTransient<ICommandHandler<FailingVoidCommand>, FailingVoidCommandHandler>();
            services.AddTransient<IQueryHandler<FailingQuery, string>, FailingQueryHandler>();
            services.AddTransient<INotificationHandler<FailingNotification>, FailingNotificationHandler>();

            configure?.Invoke(services);

            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task Command_WhenExceptionAndNoHandler_ShouldRethrow()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await mediator.SendAsync(new FailingCommand()));
        }

        [Fact]
        public async Task Command_WhenExceptionAndHandlerReturnsResult_ShouldReturnFallback()
        {
            // Arrange
            var exceptionHandler = new TestExceptionHandlerWithResult
            {
                ShouldHandle = true,
                FallbackResult = "Handled"
            };

            var provider = CreateServiceProvider(services =>
            {
                services.AddSingleton<IExceptionHandler<string>>(exceptionHandler);
            });

            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            var result = await mediator.SendAsync(new FailingCommand());

            // Assert
            Assert.Equal("Handled", result);
            Assert.Single(exceptionHandler.HandledExceptions);
        }

        [Fact]
        public async Task Command_WhenNoException_ShouldReturnNormalResult()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            var result = await mediator.SendAsync(new FailingCommand { ShouldFail = false });

            // Assert
            Assert.Equal("Success", result);
        }

        [Fact]
        public async Task VoidCommand_WhenExceptionAndHandled_ShouldComplete()
        {
            // Arrange
            var exceptionHandler = new TestExceptionHandler { ShouldHandle = true };

            var provider = CreateServiceProvider(services =>
            {
                services.AddSingleton<IExceptionHandler>(exceptionHandler);
            });

            var mediator = provider.GetRequiredService<IMediator>();

            // Act - should not throw
            await mediator.SendAsync(new FailingVoidCommand());

            // Assert
            Assert.Single(exceptionHandler.HandledExceptions);
        }

        [Fact]
        public async Task Query_WhenExceptionAndHandled_ShouldReturnFallback()
        {
            // Arrange
            var exceptionHandler = new TestExceptionHandlerWithResult
            {
                ShouldHandle = true,
                FallbackResult = "QueryFallback"
            };

            var provider = CreateServiceProvider(services =>
            {
                services.AddSingleton<IExceptionHandler<string>>(exceptionHandler);
            });

            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            var result = await mediator.QueryAsync(new FailingQuery());

            // Assert
            Assert.Equal("QueryFallback", result);
        }

        [Fact]
        public async Task Notification_WhenExceptionAndNoHandler_ShouldRethrow()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await mediator.PublishAsync(new FailingNotification()));
        }

        [Fact]
        public async Task Notification_WhenExceptionAndHandled_ShouldComplete()
        {
            // Arrange
            var exceptionHandler = new TestExceptionHandler { ShouldHandle = true };

            var provider = CreateServiceProvider(services =>
            {
                services.AddSingleton<IExceptionHandler>(exceptionHandler);
            });

            var mediator = provider.GetRequiredService<IMediator>();

            // Act - should not throw
            await mediator.PublishAsync(new FailingNotification());

            // Assert
            Assert.Single(exceptionHandler.HandledExceptions);
        }

        // Sequential publishing: the failing handler runs first, then SecondNotificationHandler.
        private static IServiceProvider CreateSuppressionServiceProvider(
            Action<MediatorOptions> configureMediator,
            Action<IServiceCollection>? configureServices = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
            services.AddSingleton<NotificationRunLog>();

            services.AddCortexMediator(Array.Empty<Type>(), options =>
            {
                options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();
                configureMediator(options);
            });

            services.AddTransient<INotificationHandler<FailingNotification>, FailingNotificationHandler>();
            services.AddTransient<INotificationHandler<FailingNotification>, SecondNotificationHandler>();

            configureServices?.Invoke(services);

            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task SuppressNotificationExceptions_ConfiguredThroughOptions_LetsOtherHandlersRun()
        {
            // Arrange
            var provider = CreateSuppressionServiceProvider(options =>
                options.AddExceptionHandlingBehaviors(o => o.SuppressNotificationExceptions = true));
            var mediator = provider.GetRequiredService<IMediator>();

            // Act - should not throw
            await mediator.PublishAsync(new FailingNotification());

            // Assert
            Assert.Equal(new[] { "second" }, provider.GetRequiredService<NotificationRunLog>().Entries);
        }

        [Fact]
        public async Task SuppressNotificationExceptions_ConfiguredThroughServicesConfigure_LetsOtherHandlersRun()
        {
            // Arrange
            var provider = CreateSuppressionServiceProvider(
                options => options.AddExceptionHandlingBehaviors(),
                services => services.Configure<ExceptionHandlingOptions>(o => o.SuppressNotificationExceptions = true));
            var mediator = provider.GetRequiredService<IMediator>();

            // Act - should not throw
            await mediator.PublishAsync(new FailingNotification());

            // Assert
            Assert.Equal(new[] { "second" }, provider.GetRequiredService<NotificationRunLog>().Entries);
        }

        [Fact]
        public async Task SuppressNotificationExceptions_Default_Rethrows()
        {
            // Arrange
            var provider = CreateSuppressionServiceProvider(options => options.AddExceptionHandlingBehaviors());
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert: the sequential strategy stops at the failure
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await mediator.PublishAsync(new FailingNotification()));
            Assert.Empty(provider.GetRequiredService<NotificationRunLog>().Entries);
        }

        [Fact]
        public async Task AddDefaultBehaviorsWithExceptionHandling_ShouldRegisterBothBehaviors()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

            // Use empty array to avoid auto-scanning for handlers
            services.AddCortexMediator(
                Array.Empty<Type>(),
                options => options.AddDefaultBehaviorsWithExceptionHandling());

            services.AddTransient<ICommandHandler<FailingCommand, string>, FailingCommandHandler>();

            var exceptionHandler = new TestExceptionHandlerWithResult
            {
                ShouldHandle = true,
                FallbackResult = "Handled"
            };
            services.AddSingleton<IExceptionHandler<string>>(exceptionHandler);

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            var result = await mediator.SendAsync(new FailingCommand());

            // Assert
            Assert.Equal("Handled", result);
        }
    }
}
