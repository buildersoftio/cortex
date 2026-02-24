using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Pipeline Behavior Ordering

    // --- Commands ---

    public class OrderTestCommand : ICommand<string>
    {
        public string Value { get; set; } = string.Empty;
    }

    public class OrderTestCommandHandler : ICommandHandler<OrderTestCommand, string>
    {
        private readonly List<string> _log;

        public OrderTestCommandHandler(List<string> log)
        {
            _log = log;
        }

        public Task<string> Handle(OrderTestCommand command, CancellationToken cancellationToken)
        {
            _log.Add("Handler");
            return Task.FromResult(command.Value);
        }
    }

    public class OrderTestVoidCommand : ICommand
    {
        public string Value { get; set; } = string.Empty;
    }

    public class OrderTestVoidCommandHandler : ICommandHandler<OrderTestVoidCommand>
    {
        private readonly List<string> _log;

        public OrderTestVoidCommandHandler(List<string> log)
        {
            _log = log;
        }

        public Task Handle(OrderTestVoidCommand command, CancellationToken cancellationToken)
        {
            _log.Add("Handler");
            return Task.CompletedTask;
        }
    }

    // --- Queries ---

    public class OrderTestQuery : IQuery<string>
    {
        public string Value { get; set; } = string.Empty;
    }

    public class OrderTestQueryHandler : IQueryHandler<OrderTestQuery, string>
    {
        private readonly List<string> _log;

        public OrderTestQueryHandler(List<string> log)
        {
            _log = log;
        }

        public Task<string> Handle(OrderTestQuery query, CancellationToken cancellationToken)
        {
            _log.Add("Handler");
            return Task.FromResult(query.Value);
        }
    }

    // --- Notifications ---

    public class OrderTestNotification : INotification
    {
        public string Value { get; set; } = string.Empty;
    }

    public class OrderTestNotificationHandler : INotificationHandler<OrderTestNotification>
    {
        private readonly List<string> _log;

        public OrderTestNotificationHandler(List<string> log)
        {
            _log = log;
        }

        public Task Handle(OrderTestNotification notification, CancellationToken cancellationToken)
        {
            _log.Add("Handler");
            return Task.CompletedTask;
        }
    }

    // --- Command Behaviors (open generic) ---

    public class AlphaCommandBehavior<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult>
        where TCommand : ICommand<TResult>
    {
        private readonly List<string> _log;

        public AlphaCommandBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Alpha:Before");
            var result = await next();
            _log.Add("Alpha:After");
            return result;
        }
    }

    public class BetaCommandBehavior<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult>
        where TCommand : ICommand<TResult>
    {
        private readonly List<string> _log;

        public BetaCommandBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Beta:Before");
            var result = await next();
            _log.Add("Beta:After");
            return result;
        }
    }

    public class GammaCommandBehavior<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult>
        where TCommand : ICommand<TResult>
    {
        private readonly List<string> _log;

        public GammaCommandBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Gamma:Before");
            var result = await next();
            _log.Add("Gamma:After");
            return result;
        }
    }

    // --- Void Command Behaviors (open generic) ---

    public class AlphaVoidCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
        where TCommand : ICommand
    {
        private readonly List<string> _log;

        public AlphaVoidCommandBehavior(List<string> log) => _log = log;

        public async Task Handle(TCommand command, CommandHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add("Alpha:Before");
            await next();
            _log.Add("Alpha:After");
        }
    }

    public class BetaVoidCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
        where TCommand : ICommand
    {
        private readonly List<string> _log;

        public BetaVoidCommandBehavior(List<string> log) => _log = log;

        public async Task Handle(TCommand command, CommandHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add("Beta:Before");
            await next();
            _log.Add("Beta:After");
        }
    }

    // --- Query Behaviors (open generic) ---

    public class AlphaQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        private readonly List<string> _log;

        public AlphaQueryBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Alpha:Before");
            var result = await next();
            _log.Add("Alpha:After");
            return result;
        }
    }

    public class BetaQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        private readonly List<string> _log;

        public BetaQueryBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Beta:Before");
            var result = await next();
            _log.Add("Beta:After");
            return result;
        }
    }

    public class GammaQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        private readonly List<string> _log;

        public GammaQueryBehavior(List<string> log) => _log = log;

        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Add("Gamma:Before");
            var result = await next();
            _log.Add("Gamma:After");
            return result;
        }
    }

    // --- Notification Behaviors (open generic) ---

    public class AlphaNotificationBehavior<TNotification> : INotificationPipelineBehavior<TNotification>
        where TNotification : INotification
    {
        private readonly List<string> _log;

        public AlphaNotificationBehavior(List<string> log) => _log = log;

        public async Task Handle(TNotification notification, NotificationHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add("Alpha:Before");
            await next();
            _log.Add("Alpha:After");
        }
    }

    public class BetaNotificationBehavior<TNotification> : INotificationPipelineBehavior<TNotification>
        where TNotification : INotification
    {
        private readonly List<string> _log;

        public BetaNotificationBehavior(List<string> log) => _log = log;

        public async Task Handle(TNotification notification, NotificationHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add("Beta:Before");
            await next();
            _log.Add("Beta:After");
        }
    }

    #endregion

    public class PipelineBehaviorOrderingTests
    {
        #region Command Behavior Ordering Tests

        [Fact]
        public async Task CommandBehaviors_WithDifferentOrders_ShouldExecuteInOrderAscending()
        {
            // Arrange: Register Beta at order 2, then Alpha at order 1.
            // Despite Beta being registered first, Alpha (order=1) should execute first (outermost).
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(BetaCommandBehavior<,>), order: 2);
                options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>), order: 1);
            });
            services.AddTransient<ICommandHandler<OrderTestCommand, string>>(sp =>
                new OrderTestCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync<OrderTestCommand, string>(new OrderTestCommand { Value = "test" });

            // Assert: Alpha (order=1) wraps Beta (order=2) wraps Handler
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        [Fact]
        public async Task CommandBehaviors_WithSameOrder_ShouldPreserveRegistrationOrder()
        {
            // Arrange: Register Alpha then Beta, both at default order 0.
            // Registration order should be preserved.
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>));
                options.AddOpenCommandPipelineBehavior(typeof(BetaCommandBehavior<,>));
            });
            services.AddTransient<ICommandHandler<OrderTestCommand, string>>(sp =>
                new OrderTestCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync<OrderTestCommand, string>(new OrderTestCommand { Value = "test" });

            // Assert: Alpha registered first, so Alpha is outermost
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        [Fact]
        public async Task CommandBehaviors_ThreeBehaviors_ShouldRespectOrderFully()
        {
            // Arrange: Register in order Gamma(3), Alpha(1), Beta(2)
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(GammaCommandBehavior<,>), order: 3);
                options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>), order: 1);
                options.AddOpenCommandPipelineBehavior(typeof(BetaCommandBehavior<,>), order: 2);
            });
            services.AddTransient<ICommandHandler<OrderTestCommand, string>>(sp =>
                new OrderTestCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync<OrderTestCommand, string>(new OrderTestCommand { Value = "test" });

            // Assert: Alpha(1) → Beta(2) → Gamma(3) → Handler
            Assert.Equal(new[]
            {
                "Alpha:Before", "Beta:Before", "Gamma:Before",
                "Handler",
                "Gamma:After", "Beta:After", "Alpha:After"
            }, log);
        }

        [Fact]
        public async Task CommandBehaviors_NegativeOrder_ShouldExecuteBeforeZero()
        {
            // Arrange: Beta at default order 0, Alpha at order -1
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(BetaCommandBehavior<,>));
                options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>), order: -1);
            });
            services.AddTransient<ICommandHandler<OrderTestCommand, string>>(sp =>
                new OrderTestCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync<OrderTestCommand, string>(new OrderTestCommand { Value = "test" });

            // Assert: Alpha(order=-1) runs before Beta(order=0)
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        #endregion

        #region Void Command Behavior Ordering Tests

        [Fact]
        public async Task VoidCommandBehaviors_WithDifferentOrders_ShouldExecuteInOrderAscending()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(BetaVoidCommandBehavior<>), order: 2);
                options.AddOpenCommandPipelineBehavior(typeof(AlphaVoidCommandBehavior<>), order: 1);
            });
            services.AddTransient<ICommandHandler<OrderTestVoidCommand>>(sp =>
                new OrderTestVoidCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync(new OrderTestVoidCommand { Value = "test" });

            // Assert
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        #endregion

        #region Query Behavior Ordering Tests

        [Fact]
        public async Task QueryBehaviors_WithDifferentOrders_ShouldExecuteInOrderAscending()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenQueryPipelineBehavior(typeof(BetaQueryBehavior<,>), order: 2);
                options.AddOpenQueryPipelineBehavior(typeof(AlphaQueryBehavior<,>), order: 1);
            });
            services.AddTransient<IQueryHandler<OrderTestQuery, string>>(sp =>
                new OrderTestQueryHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendQueryAsync<OrderTestQuery, string>(new OrderTestQuery { Value = "test" });

            // Assert
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        [Fact]
        public async Task QueryBehaviors_ThreeBehaviors_ShouldRespectOrderFully()
        {
            // Arrange: Register in scrambled order
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenQueryPipelineBehavior(typeof(GammaQueryBehavior<,>), order: 3);
                options.AddOpenQueryPipelineBehavior(typeof(AlphaQueryBehavior<,>), order: 1);
                options.AddOpenQueryPipelineBehavior(typeof(BetaQueryBehavior<,>), order: 2);
            });
            services.AddTransient<IQueryHandler<OrderTestQuery, string>>(sp =>
                new OrderTestQueryHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendQueryAsync<OrderTestQuery, string>(new OrderTestQuery { Value = "test" });

            // Assert
            Assert.Equal(new[]
            {
                "Alpha:Before", "Beta:Before", "Gamma:Before",
                "Handler",
                "Gamma:After", "Beta:After", "Alpha:After"
            }, log);
        }

        #endregion

        #region Notification Behavior Ordering Tests

        [Fact]
        public async Task NotificationBehaviors_WithDifferentOrders_ShouldExecuteInOrderAscending()
        {
            // Arrange
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();
                options.AddOpenNotificationPipelineBehavior(typeof(BetaNotificationBehavior<>), order: 2);
                options.AddOpenNotificationPipelineBehavior(typeof(AlphaNotificationBehavior<>), order: 1);
            });
            services.AddTransient<INotificationHandler<OrderTestNotification>>(sp =>
                new OrderTestNotificationHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.PublishAsync(new OrderTestNotification { Value = "test" });

            // Assert
            Assert.Equal(new[] { "Alpha:Before", "Beta:Before", "Handler", "Beta:After", "Alpha:After" }, log);
        }

        #endregion

        #region Default Order Backward Compatibility Tests

        [Fact]
        public async Task DefaultOrder_WithoutExplicitOrder_ShouldPreserveRegistrationOrder()
        {
            // Arrange: All behaviors registered at default order (0). Should preserve registration order.
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>));
                options.AddOpenCommandPipelineBehavior(typeof(BetaCommandBehavior<,>));
                options.AddOpenCommandPipelineBehavior(typeof(GammaCommandBehavior<,>));
            });
            services.AddTransient<ICommandHandler<OrderTestCommand, string>>(sp =>
                new OrderTestCommandHandler(sp.GetRequiredService<List<string>>()));

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act
            await mediator.SendCommandAsync<OrderTestCommand, string>(new OrderTestCommand { Value = "test" });

            // Assert: Registration order preserved (Alpha, Beta, Gamma)
            Assert.Equal(new[]
            {
                "Alpha:Before", "Beta:Before", "Gamma:Before",
                "Handler",
                "Gamma:After", "Beta:After", "Alpha:After"
            }, log);
        }

        #endregion

        #region Fluent API Tests

        [Fact]
        public void AddOpenCommandPipelineBehavior_WithOrder_ShouldReturnOptionsForFluent()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act
            var result = options.AddOpenCommandPipelineBehavior(typeof(AlphaCommandBehavior<,>), order: 5);

            // Assert
            Assert.Same(options, result);
        }

        [Fact]
        public void AddOpenQueryPipelineBehavior_WithOrder_ShouldReturnOptionsForFluent()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act
            var result = options.AddOpenQueryPipelineBehavior(typeof(AlphaQueryBehavior<,>), order: 3);

            // Assert
            Assert.Same(options, result);
        }

        [Fact]
        public void AddOpenNotificationPipelineBehavior_WithOrder_ShouldReturnOptionsForFluent()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act
            var result = options.AddOpenNotificationPipelineBehavior(typeof(AlphaNotificationBehavior<>), order: 10);

            // Assert
            Assert.Same(options, result);
        }

        #endregion
    }
}
