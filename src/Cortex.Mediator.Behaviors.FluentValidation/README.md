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
    builder.Configuration,
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

### Validator (Optional, via FluentValidation) - Coming in the next release v1.8
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

- `ValidationCommandBehavior`  - Coming in the next release v1.8
- `LoggingCommandBehavior`

You can also register custom behaviors:
```csharp
options.AddOpenCommandPipelineBehavior(typeof(MyCustomBehavior<>));
```

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

- Email: support@buildersoft.io
- Website: https://buildersoft.io
- GitHub Issues: [Cortex Data Framework Issues](https://github.com/buildersoftio/cortex/issues)
- Join our Discord Community: [![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


Thank you for using Cortex Data Framework! We hope it empowers you to build scalable and efficient data processing pipelines effortlessly.

Built with ❤️ by the Buildersoft team.
