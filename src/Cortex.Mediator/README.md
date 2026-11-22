# Cortex.Mediator 🧠

**Cortex.Mediator** is a lightweight and extensible implementation of the Mediator pattern for .NET applications, designed to power clean, modular architectures like **Vertical Slice Architecture** and **CQRS**.


Built as part of the [Cortex Data Framework](https://github.com/buildersoftio/cortex), this library simplifies command and query handling with built-in support for:


- ✅ Commands & Queries
- ✅ Notifications (Events)
- ✅ Pipeline Behaviors
- ✅ FluentValidation
- ✅ Logging

---

[![GitHub License](https://img.shields.io/github/license/buildersoftio/cortex)](https://github.com/buildersoftio/cortex/blob/master/LICENSE)
[![NuGet Version](https://img.shields.io/nuget/v/Cortex.Mediator?label=Cortex.Mediator)](https://www.nuget.org/packages/Cortex.Mediator)
[![GitHub contributors](https://img.shields.io/github/contributors/buildersoftio/cortex)](https://github.com/buildersoftio/cortex)
[![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


## 🚀 Getting Started

### Install via NuGet

```bash
dotnet add package Cortex.Mediator
```

## 🛠️ Setup
In `Program.cs` or `Startup.cs`:
```csharp
builder.Services.AddCortexMediator(
    new[] { typeof(Program) }, // Assemblies to scan for handlers
    options => options.AddDefaultBehaviors() // Logging
);
```

## 📦 Folder Structure Example (Vertical Slice)
```bash
Features/
  CreateUser/
    CreateUserCommand.cs
    CreateUserCommandHandler.cs
    CreateUserValidator.cs
    CreateUserEndpoint.cs
```

## ✏️ Defining a Command

```csharp
public class CreateUserCommand : ICommand<Guid>
{
    public string UserName { get; set; }
    public string Email { get; set; }
}
```

### Handler
```csharp
public class CreateUserCommandHandler : ICommandHandler<CreateUserCommand,Guid>
{
    public async Task<Guid> Handle(CreateUserCommand command, CancellationToken cancellationToken)
    {
        // Logic here
    }
}
```

### Sending Commands

**Simplified API (Recommended)** - Type is automatically inferred:
```csharp
// Using extension methods - no need to specify type parameters!
var userId = await mediator.SendAsync(command);

// For void commands (no return value)
await mediator.SendAsync(new DeleteUserCommand { UserId = userId });
```

**Explicit Type Parameters** (Legacy):
```csharp
var userId = await mediator.SendCommandAsync<CreateUserCommand, Guid>(command);
```

### Validator (Optional, via FluentValidation)
```csharp
public class CreateUserValidator : AbstractValidator<CreateUserCommand>
{
    public CreateUserValidator()
    {
        RuleFor(x => x.UserName).NotEmpty();
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
    }
}
```

---

## 🔍 Defining a Query

```csharp
public class GetUserQuery : IQuery<GetUserResponse>
{
    public int UserId { get; set; }
}
```
```csharp
public class GetUserQueryHandler : IQueryHandler<GetUserQuery, GetUserResponse>
{
    public async Task<GetUserResponse> Handle(GetUserQuery query, CancellationToken cancellationToken)
    {
        return new GetUserResponse { UserId = query.UserId, UserName = "Andy" };
    }
}

```

### Sending Queries

**Simplified API (Recommended)** - Type is automatically inferred:
```csharp
// Using extension methods - no need to specify type parameters!
var user = await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
```

**Explicit Type Parameters** (Legacy):
```csharp
var user = await mediator.SendQueryAsync<GetUserQuery, GetUserResponse>(query);
```

## 📢 Notifications (Events)

```csharp
public class UserCreatedNotification : INotification
{
    public string UserName { get; set; }
}

public class SendWelcomeEmailHandler : INotificationHandler<UserCreatedNotification>
{
    public async Task Handle(UserCreatedNotification notification, CancellationToken cancellationToken)
    {
        // Send email...
    }
}
```
```csharp
await mediator.PublishAsync(new UserCreatedNotification { UserName = "Andy" });
```

## 🔧 Pipeline Behaviors (Built-in)
Out of the box, Cortex.Mediator supports:

- `LoggingCommandBehavior` - Logs command execution with timing
- `LoggingQueryBehavior` - Logs query execution with timing
- `LoggingNotificationBehavior` - Logs notification publishing with timing
- `ExceptionHandlingCommandBehavior` - Centralized exception handling for commands
- `ExceptionHandlingQueryBehavior` - Centralized exception handling for queries
- `ExceptionHandlingNotificationBehavior` - Centralized exception handling for notifications
- `ValidationCommandBehavior` - FluentValidation support (via `Cortex.Mediator.Behaviors.FluentValidation`)

### Registering Behaviors
```csharp
// Add default logging behaviors
options.AddDefaultBehaviors();

// Add exception handling behaviors
options.AddExceptionHandlingBehaviors();

// Add both logging and exception handling
options.AddDefaultBehaviorsWithExceptionHandling();

// Custom behaviors
options.AddOpenCommandPipelineBehavior(typeof(MyCustomBehavior<,>));
options.AddOpenQueryPipelineBehavior(typeof(MyCustomQueryBehavior<,>));
options.AddOpenNotificationPipelineBehavior(typeof(MyCustomNotificationBehavior<>));
```

## ⚠️ Exception Handling Behavior
The exception handling behaviors provide centralized exception handling with optional fallback results.

### Basic Setup
```csharp
builder.Services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddExceptionHandlingBehaviors()
);
```

### Custom Exception Handler
Implement `IExceptionHandler` to customize exception handling:
```csharp
public class MyExceptionHandler : IExceptionHandler
{
    private readonly ILogger<MyExceptionHandler> _logger;

    public MyExceptionHandler(ILogger<MyExceptionHandler> logger)
    {
        _logger = logger;
    }

    public Task<bool> HandleAsync(
        Exception exception,
        Type requestType,
        object request,
        CancellationToken cancellationToken)
    {
        _logger.LogError(exception, "Error processing {RequestType}", requestType.Name);
        
        // Return true to suppress the exception, false to rethrow
        return Task.FromResult(false);
    }
}

// Register in DI
services.AddSingleton<IExceptionHandler, MyExceptionHandler>();
```

### Exception Handler with Fallback Result
For commands and queries that return a value, implement `IExceptionHandler<TResult>`:
```csharp
public class FallbackExceptionHandler : IExceptionHandler<ApiResponse>
{
    public Task<(bool handled, ApiResponse? result)> HandleWithResultAsync(
        Exception exception,
        Type requestType,
        object request,
        CancellationToken cancellationToken)
    {
        var fallback = new ApiResponse 
        { 
            Success = false, 
            Error = exception.Message 
        };
        
        return Task.FromResult((true, fallback));
    }

    public Task<bool> HandleAsync(Exception exception, Type requestType, object request, CancellationToken cancellationToken)
        => Task.FromResult(false);
}
```

### Notification Exception Suppression
For notifications, you can log a handler's exception and swallow it instead of rethrowing it:
```csharp
builder.Services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddExceptionHandlingBehaviors(o => o.SuppressNotificationExceptions = true)
);

// Or, with AddExceptionHandlingBehaviors() already registered:
builder.Services.Configure<ExceptionHandlingOptions>(o => o.SuppressNotificationExceptions = true);
```
Each handler is wrapped on its own, so with `SequentialNotificationStrategy` the remaining handlers still run after one fails, and with parallel publishing `PublishAsync` completes without an exception. A registered `IExceptionHandler` is still called first.

## 💾 Caching Behavior for Queries
The caching behavior provides automatic caching of query results to improve performance.

### Basic Setup
```csharp
// Add caching services
builder.Services.AddMediatorCaching(options =>
{
    options.DefaultAbsoluteExpiration = TimeSpan.FromMinutes(5);
    options.DefaultSlidingExpiration = TimeSpan.FromMinutes(1);
    options.CacheKeyPrefix = "MyApp";
});

// Add mediator with caching behavior
builder.Services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddCachingBehavior()
);
```

### Using the Cacheable Attribute
Mark your query classes with the `[Cacheable]` attribute:
```csharp
[Cacheable(AbsoluteExpirationSeconds = 300, SlidingExpirationSeconds = 60)]
public class GetUserQuery : IQuery<UserDto>
{
    public int UserId { get; set; }
}
```

### Using the ICacheableQuery Interface
For more control, implement `ICacheableQuery`:
```csharp
public class GetProductQuery : IQuery<ProductDto>, ICacheableQuery
{
    public int ProductId { get; set; }
    
    // Custom cache key
    public string? CacheKey => $"product-{ProductId}";
    
    // Custom expiration times
    public TimeSpan? AbsoluteExpiration => TimeSpan.FromMinutes(10);
    public TimeSpan? SlidingExpiration => TimeSpan.FromMinutes(2);
}
```

### Cache Invalidation
Use `ICacheInvalidator` to manually invalidate cached results:
```csharp
public class UpdateUserCommandHandler : ICommandHandler<UpdateUserCommand>
{
    private readonly ICacheInvalidator _cacheInvalidator;

    public UpdateUserCommandHandler(ICacheInvalidator cacheInvalidator)
    {
        _cacheInvalidator = cacheInvalidator;
    }

    public async Task Handle(UpdateUserCommand command, CancellationToken cancellationToken)
    {
        // Update user in database...
        
        // Invalidate the cached query result
        _cacheInvalidator.Invalidate<GetUserQuery, UserDto>(
            new GetUserQuery { UserId = command.UserId });
    }
}
```

### Custom Cache Key Generator
Implement `ICacheKeyGenerator` for custom key generation:
```csharp
public class MyCacheKeyGenerator : ICacheKeyGenerator
{
    public string GenerateKey<TQuery, TResult>(TQuery query) 
        where TQuery : IQuery<TResult>
    {
        // Custom key generation logic
        return $"MyApp:{typeof(TQuery).Name}:{query.GetHashCode()}";
    }
}

// Register custom generator
services.AddMediatorCaching<MyCacheKeyGenerator>();
```

## 🌊 Streaming Requests (IAsyncEnumerable)
Cortex.Mediator supports streaming queries that return `IAsyncEnumerable<T>`, perfect for handling large datasets efficiently without loading everything into memory.

### Defining a Streaming Query
```csharp
// Define the streaming query
public class GetAllUsersQuery : IStreamQuery<UserDto>
{
    public int PageSize { get; set; } = 100;
}

// Implement the streaming handler
public class GetAllUsersQueryHandler : IStreamQueryHandler<GetAllUsersQuery, UserDto>
{
    private readonly IDbConnection _db;

    public GetAllUsersQueryHandler(IDbConnection db)
    {
        _db = db;
    }

    public async IAsyncEnumerable<UserDto> Handle(
        GetAllUsersQuery query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Stream results from database one at a time
        await foreach (var user in _db.StreamUsersAsync(query.PageSize, cancellationToken))
        {
            yield return new UserDto
            {
                Id = user.Id,
                Name = user.Name,
                Email = user.Email
            };
        }
    }
}
```

### Consuming Streaming Queries
```csharp
// Using the StreamAsync extension method (recommended)
await foreach (var user in mediator.StreamAsync(new GetAllUsersQuery()))
{
    Console.WriteLine($"Processing: {user.Name}");
    // Process each user as it arrives - no need to wait for all results
}

// Or with explicit type parameters
await foreach (var user in mediator.CreateStream<GetAllUsersQuery, UserDto>(query))
{
    Console.WriteLine(user.Name);
}
```

### Streaming with Cancellation
```csharp
var cts = new CancellationTokenSource();

await foreach (var item in mediator.StreamAsync(query, cts.Token))
{
    if (ShouldStop(item))
    {
        cts.Cancel(); // Gracefully stop streaming
        break;
    }
    Process(item);
}
```

### Streaming Pipeline Behaviors
Register pipeline behaviors for streaming queries:
```csharp
// Create a custom streaming behavior
public class MetricsStreamBehavior<TQuery, TResult> : IStreamQueryPipelineBehavior<TQuery, TResult>
    where TQuery : IStreamQuery<TResult>
{
    public async IAsyncEnumerable<TResult> Handle(
        TQuery query,
        StreamQueryHandlerDelegate<TResult> next,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var count = 0;
        await foreach (var item in next().WithCancellation(cancellationToken))
        {
            count++;
            yield return item;
        }
        Console.WriteLine($"Streamed {count} items");
    }
}

// Register the behavior
services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddOpenStreamQueryPipelineBehavior(typeof(MetricsStreamBehavior<,>))
);
```

### Built-in Logging Behavior for Streams
```csharp
// Use the built-in logging behavior
options.AddOpenStreamQueryPipelineBehavior(typeof(LoggingStreamQueryBehavior<,>));
```

## 🔄 Request Pre/Post Processors
Pre-processors run before the handler executes, and post-processors run after. They're simpler than pipeline behaviors and are ideal for cross-cutting concerns.

### Basic Setup
```csharp
// Register processor behaviors
services.AddCortexMediator(
    new[] { typeof(Program) },
    options => options.AddProcessorBehaviors()
);
```

### Creating a Pre-Processor
Pre-processors run before the handler and can be used for validation, authorization, or data enrichment:
```csharp
public class LoggingPreProcessor<TRequest> : IRequestPreProcessor<TRequest>
{
    private readonly ILogger<LoggingPreProcessor<TRequest>> _logger;

    public LoggingPreProcessor(ILogger<LoggingPreProcessor<TRequest>> logger)
    {
        _logger = logger;
    }

    public Task ProcessAsync(TRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing {RequestType}", typeof(TRequest).Name);
        return Task.CompletedTask;
    }
}

// Register for a specific request type
services.AddTransient<IRequestPreProcessor<CreateOrderCommand>, OrderValidationPreProcessor>();

// Or register for all requests (generic)
services.AddTransient(typeof(IRequestPreProcessor<>), typeof(LoggingPreProcessor<>));
```

### Creating a Post-Processor
Post-processors run after successful handler execution. Use them for logging, auditing, or triggering side effects:
```csharp
// Post-processor for commands/queries that return a result
public class AuditPostProcessor<TRequest, TResponse> : IRequestPostProcessor<TRequest, TResponse>
{
    private readonly IAuditService _auditService;

    public AuditPostProcessor(IAuditService auditService)
    {
        _auditService = auditService;
    }

    public async Task ProcessAsync(TRequest request, TResponse response, CancellationToken cancellationToken)
    {
        await _auditService.LogAsync(new AuditEntry
        {
            RequestType = typeof(TRequest).Name,
            ResponseType = typeof(TResponse).Name,
            Timestamp = DateTime.UtcNow
        });
    }
}

// Post-processor for void commands
public class NotificationPostProcessor : IRequestPostProcessor<CreateOrderCommand>
{
    private readonly IMediator _mediator;

    public NotificationPostProcessor(IMediator mediator)
    {
        _mediator = mediator;
    }

    public async Task ProcessAsync(CreateOrderCommand request, CancellationToken cancellationToken)
    {
        // Publish a notification after the command completes
        await _mediator.PublishAsync(new OrderCreatedNotification { /* ... */ }, cancellationToken);
    }
}
```

### Use Cases for Pre-Processors
- **Validation**: Validate input before processing
- **Authorization**: Check user permissions
- **Data Enrichment**: Add data to the request (e.g., current user ID)
- **Rate Limiting**: Check and enforce rate limits
- **Logging**: Log incoming requests

### Use Cases for Post-Processors
- **Audit Logging**: Record what happened
- **Notifications**: Send notifications after successful operations
- **Cache Invalidation**: Clear related cached data
- **Event Publishing**: Publish domain events
- **Metrics**: Record performance metrics

## 💬 Contributing
We welcome contributions from the community! Whether it's reporting bugs, suggesting features, or submitting pull requests, your involvement helps improve Cortex for everyone.

### 💬 How to Contribute
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

## 📄 License
This project is licensed under the MIT License.

## 📚 Sponsorship
Cortex is an open-source project maintained by BuilderSoft. Your support helps us continue developing and improving Cortex. Consider sponsoring us to contribute to the future of resilient streaming platforms.

### How to Sponsor
* **Financial Contributions**: Support us through [GitHub Sponsors](https://github.com/sponsors/buildersoftio) or other preferred platforms.
* **Corporate Sponsorship**: If your organization is interested in sponsoring Cortex, please contact us directly.

Contact Us: cortex@buildersoft.io


## Contact
We'd love to hear from you! Whether you have questions, feedback, or need support, feel free to reach out.

- Email: cortex@buildersoft.io
- Website: https://buildersoft.io
- GitHub Issues: [Cortex Data Framework Issues](https://github.com/buildersoftio/cortex/issues)
- Join our Discord Community: [![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


Thank you for using Cortex Data Framework! We hope it empowers you to build scalable and efficient data processing pipelines effortlessly.

Built with ❤️ by the Buildersoft team.
