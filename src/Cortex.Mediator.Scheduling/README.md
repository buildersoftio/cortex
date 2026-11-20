# Cortex.Mediator.Scheduling

**Cortex.Mediator.Scheduling** adds deferred and scheduled command execution to Cortex.Mediator: send a reminder email in 24 hours, retry a failed payment in 5 minutes, run a command at a fixed time, without writing your own scheduling infrastructure.

Built as part of the [Cortex Data Framework](https://github.com/buildersoftio/cortex).

- Schedule an `ICommand` after a delay or at a point in time, and cancel it
- A hosted `ScheduledCommandProcessor` that polls for due commands and sends them through `IMediator`
- In-memory store for development and tests
- Database-table store for SQL Server, PostgreSQL and SQLite (any ADO.NET provider)
- Retries with a delay, a maximum number of attempts, and safe sharing of one table by several app instances
- Hooks for driving processing from Hangfire or Quartz.NET

---

[![GitHub License](https://img.shields.io/github/license/buildersoftio/cortex)](https://github.com/buildersoftio/cortex/blob/master/LICENSE)
[![NuGet Version](https://img.shields.io/nuget/v/Cortex.Mediator.Scheduling?label=Cortex.Mediator.Scheduling)](https://www.nuget.org/packages/Cortex.Mediator.Scheduling)

## Getting Started

### Install via NuGet

```bash
dotnet add package Cortex.Mediator.Scheduling
```

The package targets .NET 8, 9 and 10.

### Setup

```csharp
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Scheduling.DependencyInjection;

builder.Services.AddCortexMediator(new[] { typeof(Program) });

// In-memory store and the hosted processor, with default options
builder.Services.AddCortexMediatorScheduling();
```

The hosted processor runs inside the .NET Generic Host (`WebApplication`, `Host.CreateApplicationBuilder`, worker services).

### Schedule and cancel commands

```csharp
public class SendReminderEmailCommand : ICommand
{
    public string Email { get; set; }
}

public class CheckoutService
{
    private readonly ICommandScheduler _scheduler;

    public CheckoutService(ICommandScheduler scheduler) => _scheduler = scheduler;

    public async Task AbandonCartAsync(string email)
    {
        // Run in 24 hours
        string scheduleId = await _scheduler.ScheduleAsync(
            new SendReminderEmailCommand { Email = email },
            TimeSpan.FromHours(24));

        // Or at a fixed time
        await _scheduler.ScheduleAsync(
            new SendReminderEmailCommand { Email = email },
            new DateTimeOffset(2026, 12, 24, 9, 0, 0, TimeSpan.Zero));

        // Changed your mind? Cancel before it runs.
        await _scheduler.CancelAsync(scheduleId);
    }
}
```

When the command is due, the processor sends it with `IMediator.SendCommandAsync`, in a new dependency-injection scope, so your `ICommandHandler<SendReminderEmailCommand>` and all command pipeline behaviors run as usual.

- A delay must not be negative. A time in the past makes the command due at the next poll.
- `CancelAsync` cancels a command that has not started yet. Cancelling an unknown, running, finished or already cancelled command does nothing.

## Options

```csharp
builder.Services.AddCortexMediatorScheduling(options =>
{
    options.PollingInterval = TimeSpan.FromSeconds(5); // how often to look for due commands
    options.BatchSize = 100;                           // most commands run per poll
    options.MaxAttempts = 3;                           // 1 = no retries
    options.RetryDelay = TimeSpan.FromSeconds(30);     // wait before retrying a failed command
    options.LockDuration = TimeSpan.FromMinutes(5);    // longer than your slowest handler
    options.EnableProcessor = true;                    // false = no hosted service (see Hangfire / Quartz.NET)
});
```

Invalid values (a zero or negative interval, a batch size or attempt count below 1, a negative retry delay) throw `ArgumentOutOfRangeException` at registration.

## Stores

### In-memory (default)

`InMemoryScheduledCommandStore` keeps commands in process memory. It is meant for development and tests: commands are lost when the process stops, and the store is not shared between processes.

### Database table

`DbScheduledCommandStore` keeps commands in one table, using any ADO.NET provider. Add the provider package you already use (Microsoft.Data.SqlClient, Npgsql or Microsoft.Data.Sqlite) and register the store:

```csharp
using Cortex.Mediator.Scheduling.Stores;

// SQL Server
builder.Services.AddDbScheduledCommandStore(
    _ => new SqlConnection(connectionString),
    new SqlServerDialect(),
    options =>
    {
        options.Schema = "dbo";                   // optional
        options.TableName = "ScheduledCommands";  // default
        options.AutoCreateTable = true;           // default
    });

// PostgreSQL
builder.Services.AddDbScheduledCommandStore(_ => new NpgsqlConnection(connectionString), new PostgreSqlDialect());

// SQLite
builder.Services.AddDbScheduledCommandStore(_ => new SqliteConnection("Data Source=app.db"), new SqliteDialect());
```

`AddDbScheduledCommandStore` replaces the in-memory store, whether it is called before or after `AddCortexMediatorScheduling`. The connection factory must return a new, unopened connection; the store opens and disposes one per operation.

With `AutoCreateTable = true` the store creates the table and an index on `(Status, ExecuteAt)` on first use if they do not exist. To create them yourself (for example in a migration), set it to `false` and use these columns:

| Column | Type | Notes |
|---|---|---|
| `Id` | string, up to 64 characters, primary key | the schedule id |
| `CommandType` | string, up to 1024 characters | `Namespace.Type, AssemblyName` |
| `Payload` | text | the command as JSON |
| `ExecuteAt` | 64-bit integer | UTC ticks |
| `CreatedAt` | 64-bit integer | UTC ticks |
| `Status` | 32-bit integer | 0 Pending, 1 Processing, 2 Completed, 3 Failed, 4 Cancelled |
| `Attempts` | 32-bit integer | |
| `LastError` | text, nullable | message of the last failure |
| `LockedUntil` | 64-bit integer, nullable | UTC ticks |

`ISqlDialect.GetCreateTableSql` returns the exact DDL for each dialect. Table and schema names must be plain identifiers (letters, digits and underscores, starting with a letter or underscore); anything else throws `ArgumentException`. All values are sent as parameters.

Finished rows are kept, with their status, for auditing; delete old `Completed`, `Failed` and `Cancelled` rows yourself when you no longer need them.

The SQLite dialect is covered by the package's tests; the SQL Server and PostgreSQL dialects are not yet tested against live servers.

### Your own store

Implement `IScheduledCommandStore` and register it before or instead of the defaults:

```csharp
builder.Services.AddSingleton<IScheduledCommandStore, MyRedisScheduledCommandStore>();
builder.Services.AddCortexMediatorScheduling();
```

`ClaimDueAsync` must be atomic per command so that two processors never claim the same command at once.

## How processing works

1. Every `PollingInterval`, the processor runs up to `BatchSize` due commands: pending commands whose time has come, and commands whose lock expired. It claims them one at a time, just before running each: claiming marks a command `Processing`, locks it until now + `LockDuration` and counts an attempt. If it ran a full batch, it polls again straight away.
2. Each command is sent through `IMediator` in its own scope. On success it is marked `Completed`.
3. If the handler throws, the command goes back to `Pending`, due after `RetryDelay`, with the error in `LastError`. After `MaxAttempts` attempts it is marked `Failed`.
4. A command that cannot be read (its type no longer exists or is not an `ICommand`) is marked `Failed` without retrying.
5. If the application stops while a command runs, the command stays `Processing` and is claimed again once its lock expires. A command claimed more than `MaxAttempts` times this way is marked `Failed` without running again.

Several application instances can share one database table: each command is claimed with a conditional `UPDATE`, so only one instance runs it at a time.

Delivery is **at least once**: if the process stops after a handler finished but before the command was marked `Completed`, it runs again. Make scheduled command handlers idempotent.

### Requirements for commands

- Only commands that implement `ICommand` (no result) can be scheduled; there is no caller to receive a result later.
- Commands are stored as JSON with System.Text.Json. Use public properties with getters and setters (or constructor parameters System.Text.Json can bind). To change the JSON settings, register your own serializer:

```csharp
builder.Services.AddSingleton<IScheduledCommandSerializer>(
    new JsonScheduledCommandSerializer(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
```

- The stored type name has no assembly version, so scheduled commands survive upgrades of your assembly. Renaming or moving a command type makes its stored commands unreadable; they will be marked `Failed`.

## Hangfire and Quartz.NET

If you already run Hangfire or Quartz.NET, you can let it drive processing instead of the hosted service. Turn the hosted processor off and call `ScheduledCommandProcessor.ProcessDueCommandsAsync` from a recurring job. Each call claims and runs one batch of up to `BatchSize` commands and returns how many it claimed. `ICommandScheduler` and the store work as before.

```csharp
builder.Services.AddCortexMediatorScheduling(options => options.EnableProcessor = false);
```

The snippets below are sketches for Hangfire 1.8 and Quartz.NET 3; they are not compiled in this repository, so adapt them to your versions.

**Hangfire**

```csharp
RecurringJob.AddOrUpdate<ScheduledCommandProcessor>(
    "cortex-scheduled-commands",
    processor => processor.ProcessDueCommandsAsync(CancellationToken.None),
    Cron.Minutely());
```

**Quartz.NET**

```csharp
[DisallowConcurrentExecution]
public sealed class CortexScheduledCommandsJob : IJob
{
    private readonly ScheduledCommandProcessor _processor;

    public CortexScheduledCommandsJob(ScheduledCommandProcessor processor) => _processor = processor;

    public Task Execute(IJobExecutionContext context)
        => _processor.ProcessDueCommandsAsync(context.CancellationToken);
}

builder.Services.AddQuartz(q =>
{
    var key = new JobKey("cortex-scheduled-commands");
    q.AddJob<CortexScheduledCommandsJob>(o => o.WithIdentity(key));
    q.AddTrigger(t => t.ForJob(key).WithSimpleSchedule(s => s.WithIntervalInSeconds(5).RepeatForever()));
});
builder.Services.AddQuartzHostedService();
```

To hand scheduling itself to another system, implement `ICommandScheduler` yourself and register it before `AddCortexMediatorScheduling`; the default registration is skipped when one already exists.

## License

Cortex.Mediator.Scheduling is licensed under the MIT License. See the [LICENSE](https://github.com/buildersoftio/cortex/blob/master/LICENSE) file for details.
