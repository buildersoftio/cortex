using Cortex.Mediator.SourceGenerator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections.Immutable;
using System.Reflection;

namespace Cortex.Tests.Mediator.SourceGenerator;

public class GeneratorDriverTests
{
    private static readonly MetadataReference[] SharedReferences = GetSharedReferences();

    private static MetadataReference[] GetSharedReferences()
    {
        var assemblies = new[]
        {
            typeof(object).Assembly,
            typeof(Task).Assembly,
            typeof(IServiceProvider).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.IServiceCollection).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions).Assembly,
            typeof(Cortex.Mediator.IMediator).Assembly,
            Assembly.Load("System.Runtime"),
            Assembly.Load("System.Collections"),
            Assembly.Load("System.Linq"),
            Assembly.Load("netstandard"),
        };

        return assemblies.Select(a => MetadataReference.CreateFromFile(a.Location))
            .Cast<MetadataReference>()
            .ToArray();
    }

    private static (GeneratorDriverRunResult Result, Compilation OutputCompilation) RunGenerator(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source);

        var compilation = CSharpCompilation.Create(
            assemblyName: "TestAssembly",
            syntaxTrees: new[] { syntaxTree },
            references: SharedReferences,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new CortexMediatorGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);

        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out var outputCompilation,
            out var diagnostics);

        var result = driver.GetRunResult();
        return (result, outputCompilation);
    }

    [Fact]
    public void NoAttributeProducesNoGeneratedSource()
    {
        var source = @"
namespace TestApp
{
    public class MyClass { }
}";

        var (result, _) = RunGenerator(source);

        // Without the assembly attribute, only the attribute definition file is generated (PostInitializationOutput)
        // No GeneratedMediator or GeneratedServiceCollectionExtensions should be emitted
        var generatedFiles = result.Results[0].GeneratedSources;
        Assert.Single(generatedFiles); // Only the attribute definition
        Assert.Contains("CortexMediatorGenerationAttribute", generatedFiles[0].SourceText.ToString());
        Assert.DoesNotContain(generatedFiles, f => f.HintName == "GeneratedMediator.g.cs");
    }

    [Fact]
    public void AttributePresent_GeneratesMediator()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class CreateUserCommand : ICommand<System.Guid>
    {
        public string Name { get; set; }
    }

    public class CreateUserCommandHandler : ICommandHandler<CreateUserCommand, System.Guid>
    {
        public Task<System.Guid> Handle(CreateUserCommand command, CancellationToken cancellationToken)
        {
            return Task.FromResult(System.Guid.NewGuid());
        }
    }
}";

        var (result, outputCompilation) = RunGenerator(source);

        var generatedSources = result.Results[0].GeneratedSources;

        // Should have 3 files: attribute + GeneratedMediator + GeneratedServiceCollectionExtensions
        Assert.Equal(3, generatedSources.Length);

        var mediatorSource = generatedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        // Verify switch dispatch
        Assert.Contains("case global::TestApp.CreateUserCommand typed:", mediatorSource);
        Assert.Contains("SendCommandAsync<global::TestApp.CreateUserCommand, global::System.Guid>", mediatorSource);
    }

    [Fact]
    public void AttributePresent_GeneratesDIRegistrations()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class CreateUserCommand : ICommand<System.Guid>
    {
        public string Name { get; set; }
    }

    public class CreateUserCommandHandler : ICommandHandler<CreateUserCommand, System.Guid>
    {
        public Task<System.Guid> Handle(CreateUserCommand command, CancellationToken cancellationToken)
        {
            return Task.FromResult(System.Guid.NewGuid());
        }
    }
}";

        var (result, _) = RunGenerator(source);

        var diSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedServiceCollectionExtensions.g.cs")
            .SourceText.ToString();

        Assert.Contains("AddCortexGeneratedMediator", diSource);
        Assert.Contains("GeneratedMediator", diSource);
        Assert.Contains("global::TestApp.CreateUserCommandHandler", diSource);
        Assert.Contains("ServiceLifetime.Scoped", diSource);
    }

    [Fact]
    public void VoidCommand_GeneratesSwitchDispatch()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class DeleteUserCommand : ICommand
    {
        public int UserId { get; set; }
    }

    public class DeleteUserCommandHandler : ICommandHandler<DeleteUserCommand>
    {
        public Task Handle(DeleteUserCommand command, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        Assert.Contains("case global::TestApp.DeleteUserCommand typed:", mediatorSource);
    }

    [Fact]
    public void Query_GeneratesSwitchDispatch()
    {
        var source = @"
using Cortex.Mediator.Queries;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class GetUserQuery : IQuery<string>
    {
        public int UserId { get; set; }
    }

    public class GetUserQueryHandler : IQueryHandler<GetUserQuery, string>
    {
        public Task<string> Handle(GetUserQuery query, CancellationToken cancellationToken)
        {
            return Task.FromResult(""user"");
        }
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        Assert.Contains("case global::TestApp.GetUserQuery typed:", mediatorSource);
        Assert.Contains("SendQueryAsync<global::TestApp.GetUserQuery, string>", mediatorSource);
    }

    [Fact]
    public void Notification_GeneratesSwitchDispatch()
    {
        var source = @"
using Cortex.Mediator.Notifications;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class UserCreatedNotification : INotification
    {
        public int UserId { get; set; }
    }

    public class UserCreatedHandler : INotificationHandler<UserCreatedNotification>
    {
        public Task Handle(UserCreatedNotification notification, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        Assert.Contains("case global::TestApp.UserCreatedNotification typed:", mediatorSource);
    }

    [Fact]
    public void StreamQuery_GeneratesSwitchDispatch()
    {
        var source = @"
using Cortex.Mediator.Streaming;
using System.Collections.Generic;
using System.Threading;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class GetUsersStreamQuery : IStreamQuery<string>
    {
    }

    public class GetUsersStreamQueryHandler : IStreamQueryHandler<GetUsersStreamQuery, string>
    {
        public async IAsyncEnumerable<string> Handle(GetUsersStreamQuery query, System.Threading.CancellationToken cancellationToken)
        {
            yield return ""user1"";
        }
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        Assert.Contains("case global::TestApp.GetUsersStreamQuery typed:", mediatorSource);
    }

    [Fact]
    public void Processor_RegisteredInDI()
    {
        var source = @"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Processors;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyCommand : ICommand<int> { }

    public class MyCommandHandler : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand command, CancellationToken ct) => Task.FromResult(1);
    }

    public class MyPreProcessor : IRequestPreProcessor<MyCommand>
    {
        public Task ProcessAsync(MyCommand request, CancellationToken ct) => Task.CompletedTask;
    }
}";

        var (result, _) = RunGenerator(source);

        var diSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedServiceCollectionExtensions.g.cs")
            .SourceText.ToString();

        Assert.Contains("global::TestApp.MyPreProcessor", diSource);
        Assert.Contains("ServiceLifetime.Transient", diSource);
    }

    [Fact]
    public void MultipleHandlers_AllIncludedInSwitch()
    {
        var source = @"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Queries;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class CommandA : ICommand<int> { }
    public class CommandB : ICommand<string> { }
    public class QueryA : IQuery<bool> { }

    public class HandlerA : ICommandHandler<CommandA, int>
    {
        public Task<int> Handle(CommandA command, CancellationToken ct) => Task.FromResult(1);
    }

    public class HandlerB : ICommandHandler<CommandB, string>
    {
        public Task<string> Handle(CommandB command, CancellationToken ct) => Task.FromResult(""b"");
    }

    public class QueryHandlerA : IQueryHandler<QueryA, bool>
    {
        public Task<bool> Handle(QueryA query, CancellationToken ct) => Task.FromResult(true);
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        Assert.Contains("case global::TestApp.CommandA typed:", mediatorSource);
        Assert.Contains("case global::TestApp.CommandB typed:", mediatorSource);
        Assert.Contains("case global::TestApp.QueryA typed:", mediatorSource);
    }

    [Fact]
    public void GeneratedCode_CompilesToValidOutput()
    {
        var source = @"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Notifications;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class CreateCommand : ICommand<int> { }
    public class DeleteCommand : ICommand { }
    public class GetQuery : IQuery<string> { }
    public class AlertNotification : INotification { }

    public class CreateHandler : ICommandHandler<CreateCommand, int>
    {
        public Task<int> Handle(CreateCommand cmd, CancellationToken ct) => Task.FromResult(42);
    }
    public class DeleteHandler : ICommandHandler<DeleteCommand>
    {
        public Task Handle(DeleteCommand cmd, CancellationToken ct) => Task.CompletedTask;
    }
    public class GetHandler : IQueryHandler<GetQuery, string>
    {
        public Task<string> Handle(GetQuery q, CancellationToken ct) => Task.FromResult(""ok"");
    }
    public class AlertHandler : INotificationHandler<AlertNotification>
    {
        public Task Handle(AlertNotification n, CancellationToken ct) => Task.CompletedTask;
    }
}";

        var (result, outputCompilation) = RunGenerator(source);

        var diagnostics = outputCompilation.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToArray();

        Assert.Empty(diagnostics);
    }
}
