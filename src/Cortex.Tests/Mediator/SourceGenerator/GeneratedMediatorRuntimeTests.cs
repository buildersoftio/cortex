namespace Cortex.Tests.Mediator.SourceGenerator;

/// <summary>
/// Runs code produced by the source generator: each fixture is compiled with the generator,
/// loaded, and its <c>TestApp.Scenario.Run()</c> resolves the generated mediator from DI and dispatches through it.
/// </summary>
public class GeneratedMediatorRuntimeTests
{
    private const string OptInHeader = @"
using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Processors;
using Cortex.Mediator.Queries;
using Cortex.Mediator.SourceGeneration;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

[assembly: CortexMediatorGeneration]
";

    private const string SharedTypes = @"
namespace TestApp
{
    public sealed class Log
    {
        public List<string> Entries { get; } = new List<string>();
    }

    public class CreateOrder : ICommand<int> { public int Quantity { get; set; } }

    public class CreateOrderHandler : ICommandHandler<CreateOrder, int>
    {
        private readonly Log _log;
        public CreateOrderHandler(Log log) => _log = log;

        public Task<int> Handle(CreateOrder command, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""handler"");
            return Task.FromResult(command.Quantity * 10);
        }
    }
}
";

    [Fact]
    public async Task CommandsAndQueries_DispatchThroughTypedAndInterfaceOverloads()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class CancelOrder : ICommand { public int Id { get; set; } }

    public class CancelOrderHandler : ICommandHandler<CancelOrder>
    {
        private readonly Log _log;
        public CancelOrderHandler(Log log) => _log = log;

        public Task Handle(CancelOrder command, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""cancel:"" + command.Id);
            return Task.CompletedTask;
        }
    }

    public class GetName : IQuery<string> { public int Id { get; set; } }

    public class GetNameHandler : IQueryHandler<GetName, string>
    {
        public Task<string> Handle(GetName query, CancellationToken cancellationToken) =>
            Task.FromResult(""name-"" + query.Id);
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var log = provider.GetRequiredService<Log>();

            var typed = await mediator.SendCommandAsync<CreateOrder, int>(new CreateOrder { Quantity = 2 });
            var viaInterface = await mediator.SendCommandAsync(new CreateOrder { Quantity = 3 });
            await mediator.SendCommandAsync(new CancelOrder { Id = 7 });
            await mediator.SendCommandAsync((ICommand)new CancelOrder { Id = 8 });
            var typedQuery = await mediator.SendQueryAsync<GetName, string>(new GetName { Id = 1 });
            var interfaceQuery = await mediator.SendQueryAsync(new GetName { Id = 2 });

            var parts = new List<string> { mediator.GetType().Name, typed.ToString(), viaInterface.ToString(), typedQuery, interfaceQuery };
            parts.AddRange(log.Entries.Where(e => e != ""handler""));
            return string.Join(""|"", parts);
        }
    }
}");

        Assert.Equal("GeneratedMediator|20|30|name-1|name-2|cancel:7|cancel:8", output);
    }

    [Fact]
    public async Task Streams_YieldItemsThroughBothOverloads_AndHandlersReceiveTheCallersToken()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + @"
namespace TestApp
{
    public class Numbers : IStreamQuery<int> { public int Count { get; set; } }

    public class NumbersHandler : IStreamQueryHandler<Numbers, int>
    {
        public static CancellationToken Seen;

        public async IAsyncEnumerable<int> Handle(Numbers query, CancellationToken cancellationToken)
        {
            Seen = cancellationToken;
            for (var i = 1; i <= query.Count; i++)
            {
                await Task.Yield();
                yield return i;
            }
        }
    }

    public class Probe : ICommand<bool> { }

    public class ProbeHandler : ICommandHandler<Probe, bool>
    {
        public static CancellationToken Seen;

        public Task<bool> Handle(Probe command, CancellationToken cancellationToken)
        {
            Seen = cancellationToken;
            return Task.FromResult(true);
        }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddCortexGeneratedMediator();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            using var cts = new CancellationTokenSource();

            var typedItems = new List<int>();
            await foreach (var item in mediator.CreateStream<Numbers, int>(new Numbers { Count = 3 }, cts.Token))
                typedItems.Add(item);
            var typedToken = NumbersHandler.Seen == cts.Token;

            NumbersHandler.Seen = default;
            var interfaceItems = new List<int>();
            await foreach (var item in mediator.CreateStream(new Numbers { Count = 2 }, cts.Token))
                interfaceItems.Add(item);
            var interfaceToken = NumbersHandler.Seen == cts.Token;

            await mediator.SendCommandAsync(new Probe(), cts.Token);
            var commandToken = ProbeHandler.Seen == cts.Token;

            return string.Join(""|"", string.Join("","", typedItems), string.Join("","", interfaceItems),
                typedToken, interfaceToken, commandToken);
        }
    }
}");

        Assert.Equal("1,2,3|1,2|True|True|True", output);
    }

    [Fact]
    public async Task Notifications_FanOutToEveryHandler_ThroughBothOverloads()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class OrderPlaced : INotification { public int Id { get; set; } }

    public class EmailHandler : INotificationHandler<OrderPlaced>
    {
        private readonly Log _log;
        public EmailHandler(Log log) => _log = log;

        public Task Handle(OrderPlaced notification, CancellationToken cancellationToken)
        {
            lock (_log) _log.Entries.Add(""email:"" + notification.Id);
            return Task.CompletedTask;
        }
    }

    public class AuditHandler : INotificationHandler<OrderPlaced>
    {
        private readonly Log _log;
        public AuditHandler(Log log) => _log = log;

        public Task Handle(OrderPlaced notification, CancellationToken cancellationToken)
        {
            lock (_log) _log.Entries.Add(""audit:"" + notification.Id);
            return Task.CompletedTask;
        }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator(options => options.UseNotificationPublishStrategy<SequentialNotificationStrategy>());

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            await mediator.PublishAsync(new OrderPlaced { Id = 1 });
            await mediator.PublishAsync((INotification)new OrderPlaced { Id = 2 });

            var strategy = provider.GetRequiredService<INotificationPublishStrategy>().GetType().Name;
            var entries = provider.GetRequiredService<Log>().Entries.OrderBy(e => e, StringComparer.Ordinal);
            return strategy + ""|"" + string.Join("","", entries);
        }
    }
}");

        Assert.Equal("SequentialNotificationStrategy|audit:1,audit:2,email:1,email:2", output);
    }

    [Fact]
    public async Task Notifications_HandlerThrowsSynchronously_ReachesStrategyAsFaultedTask()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class OrderPlaced : INotification { }

    public class ThrowingHandler : INotificationHandler<OrderPlaced>
    {
        public Task Handle(OrderPlaced notification, CancellationToken cancellationToken)
            => throw new InvalidOperationException(""sync"");
    }

    public class OkHandler : INotificationHandler<OrderPlaced>
    {
        private readonly Log _log;
        public OkHandler(Log log) => _log = log;

        public Task Handle(OrderPlaced notification, CancellationToken cancellationToken)
        {
            lock (_log) _log.Entries.Add(""ok"");
            return Task.CompletedTask;
        }
    }

    // Starts every delegate and records how each failure arrives.
    public sealed class RecordingStrategy : INotificationPublishStrategy
    {
        public static int SyncThrows;
        public static int Faulted;

        public async Task PublishAsync(IEnumerable<Func<Task>> handlerDelegates, CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();
            foreach (var handler in handlerDelegates)
            {
                try { tasks.Add(handler()); }
                catch { SyncThrows++; }
            }

            foreach (var task in tasks)
            {
                try { await task; }
                catch { Faulted++; }
            }
        }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator(options => options.UseNotificationPublishStrategy<RecordingStrategy>());

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            await mediator.PublishAsync(new OrderPlaced());

            var entries = provider.GetRequiredService<Log>().Entries;
            return RecordingStrategy.SyncThrows + ""|"" + RecordingStrategy.Faulted + ""|"" + string.Join("","", entries);
        }
    }
}");

        Assert.Equal("0|1|ok", output);
    }

    [Fact]
    public async Task PipelineBehaviors_WrapHandlersInConfiguredOrder_ForCommandsAndQueries()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class Ping : ICommand { }

    public class PingHandler : ICommandHandler<Ping>
    {
        private readonly Log _log;
        public PingHandler(Log log) => _log = log;

        public Task Handle(Ping command, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""ping"");
            return Task.CompletedTask;
        }
    }

    public class Lookup : IQuery<int> { }

    public class LookupHandler : IQueryHandler<Lookup, int>
    {
        private readonly Log _log;
        public LookupHandler(Log log) => _log = log;

        public Task<int> Handle(Lookup query, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""lookup"");
            return Task.FromResult(5);
        }
    }

    public sealed class Outer<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult> where TCommand : ICommand<TResult>
    {
        private readonly Log _log;
        public Outer(Log log) => _log = log;

        public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""outer>"");
            var result = await next();
            _log.Entries.Add(""<outer"");
            return result;
        }
    }

    public sealed class Inner<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult> where TCommand : ICommand<TResult>
    {
        private readonly Log _log;
        public Inner(Log log) => _log = log;

        public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""inner>"");
            var result = await next();
            _log.Entries.Add(""<inner"");
            return result;
        }
    }

    public sealed class VoidAround<TCommand> : ICommandPipelineBehavior<TCommand> where TCommand : ICommand
    {
        private readonly Log _log;
        public VoidAround(Log log) => _log = log;

        public async Task Handle(TCommand command, CommandHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""void>"");
            await next();
            _log.Entries.Add(""<void"");
        }
    }

    public sealed class QueryAround<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult> where TQuery : IQuery<TResult>
    {
        private readonly Log _log;
        public QueryAround(Log log) => _log = log;

        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""query>"");
            var result = await next();
            _log.Entries.Add(""<query"");
            return result;
        }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator(options => options
                .AddOpenCommandPipelineBehavior(typeof(Inner<,>), order: 1)
                .AddOpenCommandPipelineBehavior(typeof(Outer<,>), order: 0)
                .AddOpenCommandPipelineBehavior(typeof(VoidAround<>))
                .AddOpenQueryPipelineBehavior(typeof(QueryAround<,>)));

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var log = provider.GetRequiredService<Log>();

            var result = await mediator.SendCommandAsync(new CreateOrder { Quantity = 4 });
            log.Entries.Add(""="" + result);
            await mediator.SendCommandAsync((ICommand)new Ping());
            var lookup = await mediator.SendQueryAsync(new Lookup());
            log.Entries.Add(""="" + lookup);

            return string.Join(""|"", log.Entries);
        }
    }
}");

        Assert.Equal(
            "outer>|inner>|handler|<inner|<outer|=40|void>|ping|<void|query>|lookup|<query|=5",
            output);
    }

    [Fact]
    public async Task Processors_RegisteredByGeneratedDI_RunAroundTheHandler()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class AuditPre : IRequestPreProcessor<CreateOrder>
    {
        private readonly Log _log;
        public AuditPre(Log log) => _log = log;

        public Task ProcessAsync(CreateOrder request, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""pre:"" + request.Quantity);
            return Task.CompletedTask;
        }
    }

    public class AuditPost : IRequestPostProcessor<CreateOrder, int>
    {
        private readonly Log _log;
        public AuditPost(Log log) => _log = log;

        public Task ProcessAsync(CreateOrder request, int response, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""post:"" + response);
            return Task.CompletedTask;
        }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator(options => options.AddProcessorBehaviors());

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            await mediator.SendCommandAsync(new CreateOrder { Quantity = 1 });
            return string.Join(""|"", provider.GetRequiredService<Log>().Entries);
        }
    }
}");

        Assert.Equal("pre:1|handler|post:10", output);
    }

    [Fact]
    public async Task GeneratedRegistrations_AreScoped_AndResolvePerScope()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public static class Scenario
    {
        public static Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();

            var handlerLifetime = services.Single(d => d.ServiceType == typeof(ICommandHandler<CreateOrder, int>)).Lifetime;
            var mediatorLifetime = services.Single(d => d.ServiceType == typeof(IMediator)).Lifetime;

            using var provider = services.BuildServiceProvider();
            using var first = provider.CreateScope();
            using var second = provider.CreateScope();
            var a = first.ServiceProvider.GetRequiredService<ICommandHandler<CreateOrder, int>>();
            var b = first.ServiceProvider.GetRequiredService<ICommandHandler<CreateOrder, int>>();
            var c = second.ServiceProvider.GetRequiredService<ICommandHandler<CreateOrder, int>>();

            return Task.FromResult(string.Join(""|"", handlerLifetime, mediatorLifetime,
                ReferenceEquals(a, b), ReferenceEquals(a, c)));
        }
    }
}");

        Assert.Equal("Scoped|Scoped|True|False", output);
    }

    [Fact]
    public async Task RecordStructAndPartialDeclarations_AreRegisteredAndDispatched()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public record Ship(int Quantity) : ICommand<int>;

    public sealed record ShipHandler : ICommandHandler<Ship, int>
    {
        public Task<int> Handle(Ship command, CancellationToken cancellationToken) => Task.FromResult(command.Quantity + 1);
    }

    public struct Measure : IQuery<int> { public int Value { get; set; } }

    public class MeasureHandler : IQueryHandler<Measure, int>
    {
        public Task<int> Handle(Measure query, CancellationToken cancellationToken) => Task.FromResult(query.Value * 2);
    }

    public class Ping : ICommand { }

    public partial class PingHandler : ICommandHandler<Ping> { }

    public partial class PingHandler : IDisposable
    {
        private readonly Log _log;
        public PingHandler(Log log) => _log = log;

        public Task Handle(Ping command, CancellationToken cancellationToken)
        {
            _log.Entries.Add(""ping"");
            return Task.CompletedTask;
        }

        public void Dispose() { }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            var shipped = await mediator.SendCommandAsync(new Ship(5));
            var measured = await mediator.SendQueryAsync(new Measure { Value = 4 });
            await mediator.SendCommandAsync((ICommand)new Ping());

            var pings = provider.GetRequiredService<Log>().Entries.Count(e => e == ""ping"");
            return string.Join(""|"", shipped, measured, pings);
        }
    }
}");

        Assert.Equal("6|8|1", output);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DerivedMessages_DispatchOnTheExactRuntimeType_InEitherDeclarationOrder(bool baseHandlersFirst)
    {
        const string baseHandlers = @"
    public class ChargeHandler : ICommandHandler<Charge, string>
    {
        public Task<string> Handle(Charge command, CancellationToken cancellationToken) => Task.FromResult(""charge"");
    }

    public class HappenedHandler : INotificationHandler<Happened>
    {
        private readonly Log _log;
        public HappenedHandler(Log log) => _log = log;
        public Task Handle(Happened notification, CancellationToken cancellationToken) { _log.Entries.Add(""base-event""); return Task.CompletedTask; }
    }
";
        const string derivedHandlers = @"
    public class RefundHandler : ICommandHandler<Refund, string>
    {
        public Task<string> Handle(Refund command, CancellationToken cancellationToken) => Task.FromResult(""refund"");
    }

    public class HappenedAgainHandler : INotificationHandler<HappenedAgain>
    {
        private readonly Log _log;
        public HappenedAgainHandler(Log log) => _log = log;
        public Task Handle(HappenedAgain notification, CancellationToken cancellationToken) { _log.Entries.Add(""derived-event""); return Task.CompletedTask; }
    }
";

        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class Charge : ICommand<string> { }
    public class Refund : Charge { }
    public class Unrouted : Charge { }
    public class Happened : INotification { }
    public class HappenedAgain : Happened { }
" + (baseHandlersFirst ? baseHandlers + derivedHandlers : derivedHandlers + baseHandlers) + @"
    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var log = provider.GetRequiredService<Log>();

            var parts = new List<string>
            {
                await mediator.SendCommandAsync(new Charge()),
                await mediator.SendCommandAsync(new Refund()),
            };

            try
            {
                parts.Add(await mediator.SendCommandAsync(new Unrouted()));
            }
            catch (InvalidOperationException)
            {
                parts.Add(""unrouted-rejected"");
            }

            await mediator.PublishAsync((INotification)new Happened());
            await mediator.PublishAsync((INotification)new HappenedAgain());
            parts.AddRange(log.Entries);
            return string.Join(""|"", parts);
        }
    }
}");

        Assert.Equal("charge|refund|unrouted-rejected|base-event|derived-event", output);
    }

    [Fact]
    public async Task HandlerLifetimeOption_AppliesToGeneratedHandlerRegistrations()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class AuditPre : IRequestPreProcessor<CreateOrder>
    {
        public Task ProcessAsync(CreateOrder request, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public static class Scenario
    {
        public static Task<string> Run()
        {
            var parts = new List<string>();
            foreach (var lifetime in new[] { ServiceLifetime.Singleton, ServiceLifetime.Transient, ServiceLifetime.Scoped })
            {
                var services = new ServiceCollection();
                services.AddCortexGeneratedMediator(options => options.HandlerLifetime = lifetime);

                var handler = services.Single(d => d.ServiceType == typeof(ICommandHandler<CreateOrder, int>)).Lifetime;
                var processor = services.Single(d => d.ServiceType == typeof(IRequestPreProcessor<CreateOrder>)).Lifetime;
                var mediator = services.Single(d => d.ServiceType == typeof(IMediator)).Lifetime;
                parts.Add(handler + ""/"" + processor + ""/"" + mediator);
            }

            return Task.FromResult(string.Join(""|"", parts));
        }
    }
}");

        Assert.Equal("Singleton/Transient/Scoped|Transient/Transient/Scoped|Scoped/Transient/Scoped", output);
    }

    [Fact]
    public async Task DiscoveredNotificationsWithoutGeneratedHandlers_PublishThroughBothOverloads()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class Quiet : INotification { }
    public class Lonely : INotification { }

    // Neither of these may break the generated switch.
    public class Envelope<T> : INotification { }
    public class Box { private class Secret : INotification { } }

    // Generic handlers are not discovered, so this one is only reachable through a manual registration.
    public class Catch<T> : INotificationHandler<T> where T : INotification
    {
        private readonly Log _log;
        public Catch(Log log) => _log = log;
        public Task Handle(T notification, CancellationToken cancellationToken) { _log.Entries.Add(""caught:"" + typeof(T).Name); return Task.CompletedTask; }
    }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();
            services.AddTransient<INotificationHandler<Lonely>, Catch<Lonely>>();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var log = provider.GetRequiredService<Log>();

            await mediator.PublishAsync(new Quiet());
            log.Entries.Add(""quiet-typed"");
            await mediator.PublishAsync((INotification)new Quiet());
            log.Entries.Add(""quiet-interface"");
            await mediator.PublishAsync((INotification)new Lonely());
            return string.Join(""|"", log.Entries);
        }
    }
}");

        Assert.Equal("quiet-typed|quiet-interface|caught:Lonely", output);
    }

    [Fact]
    public async Task UnknownAndNullRequests_FailWithClearExceptions()
    {
        var output = await GeneratorTestHarness.RunScenarioAsync(OptInHeader + SharedTypes + @"
namespace TestApp
{
    public class Orphan : ICommand<int> { }
    public class OrphanQuery : IQuery<int> { }
    public class OrphanStream : IStreamQuery<int> { }

    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexGeneratedMediator();

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var outcomes = new List<string>();

            outcomes.Add(await Capture(() => mediator.SendCommandAsync(new Orphan())));
            outcomes.Add(await Capture(() => mediator.SendQueryAsync(new OrphanQuery())));
            outcomes.Add(await Capture(() => Task.FromResult(mediator.CreateStream(new OrphanStream()))));
            outcomes.Add(await Capture(() => mediator.SendCommandAsync((ICommand<int>)null)));
            outcomes.Add(await Capture(() => mediator.PublishAsync((INotification)null)));
            return string.Join(""|"", outcomes);
        }

        private static async Task<string> Capture(Func<Task> action)
        {
            try
            {
                await action();
                return ""none"";
            }
            catch (Exception ex)
            {
                return ex.GetType().Name;
            }
        }
    }
}");

        Assert.Equal(
            "InvalidOperationException|InvalidOperationException|InvalidOperationException|ArgumentNullException|ArgumentNullException",
            output);
    }

    [Fact]
    public async Task WithoutOptIn_NothingIsGenerated_AndTheReflectionMediatorStillWorks()
    {
        var assembly = GeneratorTestHarness.CompileAndLoad(@"
using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
" + SharedTypes + @"
namespace TestApp
{
    public static class Scenario
    {
        public static async Task<string> Run()
        {
            var services = new ServiceCollection();
            services.AddSingleton<Log>();
            services.AddCortexMediator(new[] { typeof(Scenario) });

            using var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            var result = await mediator.SendCommandAsync(new CreateOrder { Quantity = 2 });
            return mediator.GetType().Name + ""|"" + result;
        }
    }
}");

        Assert.Null(assembly.GetType("Cortex.Mediator.SourceGeneration.GeneratedMediator"));
        Assert.Null(assembly.GetType("Cortex.Mediator.SourceGeneration.GeneratedServiceCollectionExtensions"));
        Assert.Equal("Mediator|20", await GeneratorTestHarness.InvokeScenarioAsync(assembly));
    }
}
