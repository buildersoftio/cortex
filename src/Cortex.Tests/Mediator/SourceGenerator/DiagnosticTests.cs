using Cortex.Mediator.SourceGenerator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Reflection;

namespace Cortex.Tests.Mediator.SourceGenerator;

public class DiagnosticTests
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

    private static GeneratorDriverRunResult RunGenerator(string source)
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
            out _,
            out _);

        return driver.GetRunResult();
    }

    [Fact]
    public void CXMED001_CommandWithNoHandler_ProducesWarning()
    {
        var source = @"
using Cortex.Mediator.Commands;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class OrphanCommand : ICommand<int> { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED001");
    }

    [Fact]
    public void CXMED001_VoidCommandWithNoHandler_ProducesWarning()
    {
        var source = @"
using Cortex.Mediator.Commands;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class OrphanVoidCommand : ICommand { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED001");
    }

    [Fact]
    public void CXMED002_QueryWithNoHandler_ProducesWarning()
    {
        var source = @"
using Cortex.Mediator.Queries;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class OrphanQuery : IQuery<string> { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED002");
    }

    [Fact]
    public void CXMED003_StreamQueryWithNoHandler_ProducesWarning()
    {
        var source = @"
using Cortex.Mediator.Streaming;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class OrphanStreamQuery : IStreamQuery<int> { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED003");
    }

    [Fact]
    public void CXMED004_NotificationWithNoHandler_ProducesInfo()
    {
        var source = @"
using Cortex.Mediator.Notifications;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class OrphanNotification : INotification { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED004");
    }

    [Fact]
    public void CXMED005_MultipleHandlersForSameCommand_ProducesError()
    {
        var source = @"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyCommand : ICommand<int> { }

    public class Handler1 : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand cmd, CancellationToken ct) => Task.FromResult(1);
    }

    public class Handler2 : ICommandHandler<MyCommand, int>
    {
        public Task<int> Handle(MyCommand cmd, CancellationToken ct) => Task.FromResult(2);
    }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED005" && d.Severity == DiagnosticSeverity.Error);
    }

    [Fact]
    public void CXMED006_MultipleHandlersForSameQuery_ProducesError()
    {
        var source = @"
using Cortex.Mediator.Queries;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class MyQuery : IQuery<string> { }

    public class QueryHandler1 : IQueryHandler<MyQuery, string>
    {
        public Task<string> Handle(MyQuery q, CancellationToken ct) => Task.FromResult(""a"");
    }

    public class QueryHandler2 : IQueryHandler<MyQuery, string>
    {
        public Task<string> Handle(MyQuery q, CancellationToken ct) => Task.FromResult(""b"");
    }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "CXMED006" && d.Severity == DiagnosticSeverity.Error);
    }

    [Fact]
    public void NoAttribute_NoDiagnostics()
    {
        var source = @"
using Cortex.Mediator.Commands;

namespace TestApp
{
    public class OrphanCommand : ICommand<int> { }
}";

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        // No diagnostics should be produced without the assembly attribute
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void CommandWithHandler_NoDiagnostic()
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

        var result = RunGenerator(source);
        var diagnostics = result.Results[0].Diagnostics;

        Assert.DoesNotContain(diagnostics, d => d.Id == "CXMED001");
    }
}
