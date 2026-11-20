using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.SourceGeneration;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;

[assembly: CortexMediatorGeneration]

// --skip-pipelines isolates the generated dispatch from runtime-configured pipeline behaviors, which rely on
// reflection over MediatorOptions. The default run always includes them.
var withPipelines = !args.Contains("--skip-pipelines");
var failures = new List<string>();
var checks = 0;

void Check(string name, object? actual, object? expected)
{
    checks++;
    if (!Equals(actual, expected))
        failures.Add($"{name}: expected '{expected}', got '{actual}'");
}

var services = new ServiceCollection();
services.AddSingleton<SmokeLog>();
services.AddCortexGeneratedMediator(options =>
{
    options.UseNotificationPublishStrategy<SequentialNotificationStrategy>();
    if (withPipelines)
        options.AddOpenCommandPipelineBehavior(typeof(TracingBehavior<,>));
});

using var provider = services.BuildServiceProvider();
using var scope = provider.CreateScope();
var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
var log = provider.GetRequiredService<SmokeLog>();
using var cts = new CancellationTokenSource();

Check("mediator type", mediator.GetType().Name, "GeneratedMediator");

Check("command (typed)", await mediator.SendCommandAsync<Add, int>(new Add(2, 3), cts.Token), 5);
Check("command (interface)", await mediator.SendCommandAsync(new Add(4, 5), cts.Token), 9);
Check("command token", AddHandler.LastToken == cts.Token, true);
if (withPipelines)
    Check("pipeline", log.Count("trace>Add") + "/" + log.Count("<trace"), "2/2");

await mediator.SendCommandAsync((ICommand)new Reset(), cts.Token);
Check("void command (interface)", log.Count("reset"), 1);

Check("query (interface)", await mediator.SendQueryAsync(new Square(7), cts.Token), 49);

var items = new List<int>();
await foreach (var item in mediator.CreateStream(new CountTo(3), cts.Token))
    items.Add(item);
Check("stream (interface)", string.Join(",", items), "1,2,3");
Check("stream token", CountToHandler.LastToken == cts.Token, true);

await mediator.PublishAsync((INotification)new Pinged("smoke"), cts.Token);
Check("notification fan-out", log.Count("audit:smoke") + log.Count("email:smoke"), 2);

if (failures.Count > 0)
{
    foreach (var failure in failures)
        Console.Error.WriteLine("SMOKE FAIL: " + failure);
    return 1;
}

Console.WriteLine($"SMOKE OK: {checks} checks passed through {mediator.GetType().Name}" +
                  (withPipelines ? "" : " (pipeline behaviors skipped)"));
return 0;

public sealed class SmokeLog
{
    private readonly List<string> _entries = new();

    public void Add(string entry)
    {
        lock (_entries) _entries.Add(entry);
    }

    public int Count(string entry)
    {
        lock (_entries) return _entries.Count(e => e == entry);
    }
}

public sealed record Add(int Left, int Right) : ICommand<int>;

public sealed class AddHandler : ICommandHandler<Add, int>
{
    public static CancellationToken LastToken;

    public Task<int> Handle(Add command, CancellationToken cancellationToken)
    {
        LastToken = cancellationToken;
        return Task.FromResult(command.Left + command.Right);
    }
}

public sealed class Reset : ICommand { }

public sealed class ResetHandler(SmokeLog log) : ICommandHandler<Reset>
{
    public Task Handle(Reset command, CancellationToken cancellationToken)
    {
        log.Add("reset");
        return Task.CompletedTask;
    }
}

public sealed record Square(int Value) : IQuery<int>;

public sealed class SquareHandler : IQueryHandler<Square, int>
{
    public Task<int> Handle(Square query, CancellationToken cancellationToken) =>
        Task.FromResult(query.Value * query.Value);
}

public sealed record CountTo(int Limit) : IStreamQuery<int>;

public sealed class CountToHandler : IStreamQueryHandler<CountTo, int>
{
    public static CancellationToken LastToken;

    public async IAsyncEnumerable<int> Handle(CountTo query, CancellationToken cancellationToken)
    {
        LastToken = cancellationToken;
        for (var i = 1; i <= query.Limit; i++)
        {
            await Task.Yield();
            yield return i;
        }
    }
}

public sealed record Pinged(string Source) : INotification;

public sealed class AuditPinged(SmokeLog log) : INotificationHandler<Pinged>
{
    public Task Handle(Pinged notification, CancellationToken cancellationToken)
    {
        log.Add("audit:" + notification.Source);
        return Task.CompletedTask;
    }
}

public sealed class EmailPinged(SmokeLog log) : INotificationHandler<Pinged>
{
    public Task Handle(Pinged notification, CancellationToken cancellationToken)
    {
        log.Add("email:" + notification.Source);
        return Task.CompletedTask;
    }
}

public sealed class TracingBehavior<TCommand, TResult>(SmokeLog log) : ICommandPipelineBehavior<TCommand, TResult>
    where TCommand : ICommand<TResult>
{
    public async Task<TResult> Handle(TCommand command, CommandHandlerDelegate<TResult> next, CancellationToken cancellationToken)
    {
        log.Add("trace>" + typeof(TCommand).Name);
        var result = await next();
        log.Add("<trace");
        return result;
    }
}
