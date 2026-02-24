using Cortex.Mediator.SourceGenerator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Reflection;

namespace Cortex.Tests.Mediator.SourceGenerator;

public class GeneratedMediatorIntegrationTests
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
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions).Assembly,
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
            out _);

        var result = driver.GetRunResult();
        return (result, outputCompilation);
    }

    [Fact]
    public void GeneratedCode_CompilesWith_AllHandlerTypes()
    {
        var source = @"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Streaming;
using Cortex.Mediator.Processors;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    // Returning command
    public class CreateCommand : ICommand<int> { public string Name { get; set; } }
    public class CreateHandler : ICommandHandler<CreateCommand, int>
    {
        public Task<int> Handle(CreateCommand cmd, CancellationToken ct) => Task.FromResult(42);
    }

    // Void command
    public class DeleteCommand : ICommand { public int Id { get; set; } }
    public class DeleteHandler : ICommandHandler<DeleteCommand>
    {
        public Task Handle(DeleteCommand cmd, CancellationToken ct) => Task.CompletedTask;
    }

    // Query
    public class GetQuery : IQuery<string> { public int Id { get; set; } }
    public class GetHandler : IQueryHandler<GetQuery, string>
    {
        public Task<string> Handle(GetQuery q, CancellationToken ct) => Task.FromResult(""result"");
    }

    // Notification
    public class ItemCreated : INotification { public int ItemId { get; set; } }
    public class ItemCreatedHandler1 : INotificationHandler<ItemCreated>
    {
        public Task Handle(ItemCreated n, CancellationToken ct) => Task.CompletedTask;
    }
    public class ItemCreatedHandler2 : INotificationHandler<ItemCreated>
    {
        public Task Handle(ItemCreated n, CancellationToken ct) => Task.CompletedTask;
    }

    // Stream query
    public class StreamItems : IStreamQuery<string> { }
    public class StreamItemsHandler : IStreamQueryHandler<StreamItems, string>
    {
        public async IAsyncEnumerable<string> Handle(StreamItems q, CancellationToken ct)
        {
            yield return ""item1"";
            yield return ""item2"";
        }
    }

    // Pre-processor
    public class CreatePreProcessor : IRequestPreProcessor<CreateCommand>
    {
        public Task ProcessAsync(CreateCommand request, CancellationToken ct) => Task.CompletedTask;
    }

    // Post-processor
    public class CreatePostProcessor : IRequestPostProcessor<CreateCommand, int>
    {
        public Task ProcessAsync(CreateCommand request, int response, CancellationToken ct) => Task.CompletedTask;
    }

    // Void post-processor
    public class DeletePostProcessor : IRequestPostProcessor<DeleteCommand>
    {
        public Task ProcessAsync(DeleteCommand request, CancellationToken ct) => Task.CompletedTask;
    }
}";

        var (result, outputCompilation) = RunGenerator(source);

        // Check no compilation errors in generated code
        var errors = outputCompilation.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToArray();

        Assert.Empty(errors);

        // Verify all expected files were generated
        var generatedSources = result.Results[0].GeneratedSources;
        Assert.Equal(3, generatedSources.Length);

        // Verify mediator source content
        var mediatorSource = generatedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        // All switch cases should be present
        Assert.Contains("case global::TestApp.CreateCommand typed:", mediatorSource);
        Assert.Contains("case global::TestApp.DeleteCommand typed:", mediatorSource);
        Assert.Contains("case global::TestApp.GetQuery typed:", mediatorSource);
        Assert.Contains("case global::TestApp.ItemCreated typed:", mediatorSource);
        Assert.Contains("case global::TestApp.StreamItems typed:", mediatorSource);

        // Verify DI source content
        var diSource = generatedSources
            .First(s => s.HintName == "GeneratedServiceCollectionExtensions.g.cs")
            .SourceText.ToString();

        // All handlers registered
        Assert.Contains("global::TestApp.CreateHandler", diSource);
        Assert.Contains("global::TestApp.DeleteHandler", diSource);
        Assert.Contains("global::TestApp.GetHandler", diSource);
        Assert.Contains("global::TestApp.ItemCreatedHandler1", diSource);
        Assert.Contains("global::TestApp.ItemCreatedHandler2", diSource);
        Assert.Contains("global::TestApp.StreamItemsHandler", diSource);

        // Processors registered
        Assert.Contains("global::TestApp.CreatePreProcessor", diSource);
        Assert.Contains("global::TestApp.CreatePostProcessor", diSource);
        Assert.Contains("global::TestApp.DeletePostProcessor", diSource);
    }

    [Fact]
    public void GeneratedMediator_HasCorrectPipelineWrappers()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyCommand : ICommand<int> { }
    public class MyHandler : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand cmd, CancellationToken ct) => Task.FromResult(1);
    }
}";

        var (result, outputCompilation) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        // Verify pipeline wrapper classes exist
        Assert.Contains("PipelineBehaviorNextDelegate<TCommand, TResult>", mediatorSource);
        Assert.Contains("VoidPipelineBehaviorNextDelegate<TCommand>", mediatorSource);
        Assert.Contains("QueryPipelineBehaviorNextDelegate<TQuery, TResult>", mediatorSource);

        // No compilation errors
        var errors = outputCompilation.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.Empty(errors);
    }

    [Fact]
    public void GeneratedDI_RegistersPipelineBehaviors()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyCommand : ICommand<int> { }
    public class MyHandler : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand cmd, CancellationToken ct) => Task.FromResult(1);
    }
}";

        var (result, _) = RunGenerator(source);

        var diSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedServiceCollectionExtensions.g.cs")
            .SourceText.ToString();

        // Verify it calls RegisterPipelineBehaviors
        Assert.Contains("ServiceCollectionExtensions.RegisterPipelineBehaviors(services, options)", diSource);

        // Verify it registers the notification publish strategy
        Assert.Contains("options.NotificationPublishStrategyType", diSource);

        // Verify it accepts configuration action
        Assert.Contains("Action<MediatorOptions>?", diSource);
    }

    [Fact]
    public void Notification_MultipleHandlers_SingleSwitchCase()
    {
        var source = @"
using Cortex.Mediator.Notifications;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyEvent : INotification { }

    public class Handler1 : INotificationHandler<MyEvent>
    {
        public Task Handle(MyEvent n, CancellationToken ct) => Task.CompletedTask;
    }

    public class Handler2 : INotificationHandler<MyEvent>
    {
        public Task Handle(MyEvent n, CancellationToken ct) => Task.CompletedTask;
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        // Only one switch case for the notification (deduplicated)
        var caseCount = mediatorSource.Split("case global::TestApp.MyEvent typed:").Length - 1;
        Assert.Equal(1, caseCount);

        // But both handlers registered in DI
        var diSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedServiceCollectionExtensions.g.cs")
            .SourceText.ToString();

        Assert.Contains("global::TestApp.Handler1", diSource);
        Assert.Contains("global::TestApp.Handler2", diSource);
    }

    [Fact]
    public void GeneratedMediator_IncludesNullChecks()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyCommand : ICommand<int> { }
    public class MyHandler : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand cmd, CancellationToken ct) => Task.FromResult(1);
    }
}";

        var (result, _) = RunGenerator(source);

        var mediatorSource = result.Results[0].GeneratedSources
            .First(s => s.HintName == "GeneratedMediator.g.cs")
            .SourceText.ToString();

        // Non-generic overloads should have null checks
        Assert.Contains("throw new ArgumentNullException(nameof(command))", mediatorSource);
        Assert.Contains("throw new ArgumentNullException(nameof(query))", mediatorSource);
        Assert.Contains("throw new ArgumentNullException(nameof(notification))", mediatorSource);
    }
}
