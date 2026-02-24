# Cortex.Mediator.SourceGenerator

**Cortex.Mediator.SourceGenerator** is an optional Roslyn incremental source generator for [Cortex.Mediator](https://www.nuget.org/packages/Cortex.Mediator) that eliminates runtime reflection by generating compile-time dispatch.

Built as part of the [Cortex Data Framework](https://github.com/buildersoftio/cortex), this package replaces the five `ConcurrentDictionary<Type, MethodInfo>` caches and `MakeGenericMethod` + `Invoke` calls in the default `Mediator` with a `GeneratedMediator` class that uses `switch`-based routing and explicit DI registrations.

---

[![GitHub License](https://img.shields.io/github/license/buildersoftio/cortex)](https://github.com/buildersoftio/cortex/blob/master/LICENSE)
[![NuGet Version](https://img.shields.io/nuget/v/Cortex.Mediator?label=Cortex.Mediator)](https://www.nuget.org/packages/Cortex.Mediator)
[![GitHub contributors](https://img.shields.io/github/contributors/buildersoftio/cortex)](https://github.com/buildersoftio/cortex)
[![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)

## Why Use This?

| Aspect | Default `Mediator` | `GeneratedMediator` |
|--------|-------------------|---------------------|
| Non-generic dispatch | Runtime reflection (`MakeGenericMethod`) | Compile-time `switch` |
| Cold start | Dictionary allocation + reflection lookup | Zero overhead |
| DI registrations | Assembly scanning at startup | Explicit `ServiceDescriptor` calls |
| NativeAOT / Trimming | Incompatible (reflection) | Compatible |
| Pipeline behaviors | Fully supported | Fully supported (identical logic) |

## Getting Started

### Install via NuGet

```bash
dotnet add package Cortex.Mediator
dotnet add package Cortex.Mediator.SourceGenerator
```

Or in your `.csproj`:

```xml
<PackageReference Include="Cortex.Mediator" Version="1.0.0" />
<PackageReference Include="Cortex.Mediator.SourceGenerator" Version="1.0.0"
    OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
```

### Enable the Generator

Add the assembly attribute in any `.cs` file in your project:

```csharp
[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]
```

### Register Services

Replace `AddCortexMediator` with `AddCortexGeneratedMediator`:

```csharp
// Before (reflection-based):
services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddDefaultBehaviors()
);

// After (source-generated):
services.AddCortexGeneratedMediator(options =>
{
    options.AddDefaultBehaviors();
});
```

No assembly marker types needed — the generator discovers all handlers at compile time.

## What Gets Generated

### `GeneratedMediator.g.cs`

A class implementing `IMediator` with switch-based dispatch for all non-generic overloads:

```csharp
public Task<TResult> SendCommandAsync<TResult>(ICommand<TResult> command, CancellationToken ct)
{
    switch (command)
    {
        case CreateUserCommand typed:
            return (Task<TResult>)(object)SendCommandAsync<CreateUserCommand, Guid>(typed, ct);
        case UpdateUserCommand typed:
            return (Task<TResult>)(object)SendCommandAsync<UpdateUserCommand, bool>(typed, ct);
        default:
            throw new InvalidOperationException(...);
    }
}
```

The strongly-typed methods (`SendCommandAsync<TCommand, TResult>`, etc.) use the same pipeline behavior wrapping as the default `Mediator` — no reflection involved.

### `GeneratedServiceCollectionExtensions.g.cs`

Explicit DI registrations for every discovered handler and processor:

```csharp
public static IServiceCollection AddCortexGeneratedMediator(
    this IServiceCollection services,
    Action<MediatorOptions>? configure = null)
{
    services.AddScoped<IMediator, GeneratedMediator>();
    services.AddSingleton(typeof(INotificationPublishStrategy), options.NotificationPublishStrategyType);

    // Explicit handler registrations (no assembly scanning)
    services.Add(new ServiceDescriptor(
        typeof(ICommandHandler<CreateUserCommand, Guid>),
        typeof(CreateUserCommandHandler),
        ServiceLifetime.Scoped));
    // ...

    // Pipeline behaviors are still runtime-configured
    ServiceCollectionExtensions.RegisterPipelineBehaviors(services, options);
    return services;
}
```

## Compile-Time Diagnostics

The generator reports diagnostics during compilation:

| ID | Severity | Condition |
|----|----------|-----------|
| `CXMED001` | Warning | Command has no handler |
| `CXMED002` | Warning | Query has no handler |
| `CXMED003` | Warning | StreamQuery has no handler |
| `CXMED004` | Info | Notification has no handler (zero handlers is valid) |
| `CXMED005` | Error | Multiple handlers for same command |
| `CXMED006` | Error | Multiple handlers for same query |

## Supported Handler Types

The generator discovers all handler and processor implementations:

- `ICommandHandler<TCommand, TResult>` — Returning commands
- `ICommandHandler<TCommand>` — Void commands
- `IQueryHandler<TQuery, TResult>` — Queries
- `IStreamQueryHandler<TQuery, TResult>` — Streaming queries
- `INotificationHandler<TNotification>` — Notifications
- `IRequestPreProcessor<TRequest>` — Pre-processors
- `IRequestPostProcessor<TRequest, TResponse>` — Post-processors with response
- `IRequestPostProcessor<TRequest>` — Post-processors for void commands

## Scope & Limitations

- **Single compilation**: The generator scans handlers in the compilation where `[assembly: CortexMediatorGeneration]` is applied. Cross-assembly handlers are not discovered.
- **Pipeline behaviors**: Configured at runtime via `MediatorOptions`, not generated (they use open generics).
- **Fallback**: The existing reflection-based `Mediator` remains the default. The generator is opt-in.

## Contributing

We welcome contributions from the community! Whether it's reporting bugs, suggesting features, or submitting pull requests, your involvement helps improve Cortex for everyone.

### How to Contribute
1. **Fork the Repository**
2. **Create a Feature Branch**
```bash
git checkout -b feature/YourFeature
```
3. **Commit Your Changes**
```bash
git commit -m "Add your feature"
```
4. **Push to Your Fork**
```bash
git push origin feature/YourFeature
```
5. **Open a Pull Request**

Describe your changes and submit the pull request for review.

## License
This project is licensed under the MIT License.

## Contact
- Email: cortex@buildersoft.io
- Website: https://buildersoft.io
- GitHub Issues: [Cortex Data Framework Issues](https://github.com/buildersoftio/cortex/issues)
- Join our Discord Community: [![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)

Thank you for using Cortex Data Framework! We hope it empowers you to build scalable and efficient data processing pipelines effortlessly.

Built with love by the Buildersoft team.
