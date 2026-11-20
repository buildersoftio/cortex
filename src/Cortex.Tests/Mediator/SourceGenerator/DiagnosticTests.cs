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

    [Fact]
    public void RecordAndStructMessagesWithoutHandlers_ReportMissingHandlerDiagnosticsAtTheType()
    {
        var run = GeneratorTestHarness.Run(@"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public record OrphanRecordCommand(int Id) : ICommand<int>;
    public record struct OrphanRecordStructCommand : ICommand;
    public struct OrphanStructQuery : IQuery<string> { }
    public record OrphanStream : IStreamQuery<int>;
    public record OrphanNotification : INotification;
}");

        AssertSingleDiagnostic(run, "CXMED001", DiagnosticSeverity.Warning, "OrphanRecordCommand");
        AssertSingleDiagnostic(run, "CXMED001", DiagnosticSeverity.Warning, "OrphanRecordStructCommand");
        AssertSingleDiagnostic(run, "CXMED002", DiagnosticSeverity.Warning, "OrphanStructQuery");
        AssertSingleDiagnostic(run, "CXMED003", DiagnosticSeverity.Warning, "OrphanStream");
        AssertSingleDiagnostic(run, "CXMED004", DiagnosticSeverity.Info, "OrphanNotification");
    }

    [Fact]
    public void PartialTypesDeclaredInSeveralParts_AreDiscoveredOnce()
    {
        var run = GeneratorTestHarness.Run(@"
using Cortex.Mediator.Commands;
using System;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class Ping : ICommand<int> { }

    public partial class PingHandler : ICommandHandler<Ping, int> { }

    public partial class PingHandler : IDisposable
    {
        public Task<int> Handle(Ping command, CancellationToken cancellationToken) => Task.FromResult(1);
        public void Dispose() { }
    }

    public partial class Unanswered : ICommand<int> { }

    public partial class Unanswered : IComparable
    {
        public int CompareTo(object other) => 0;
    }
}");

        Assert.DoesNotContain(run.GeneratorDiagnostics, d => d.Severity == DiagnosticSeverity.Error);
        Assert.Empty(run.CompilationErrors());
        Assert.Single(run.GeneratorDiagnostics, d => d.Id == "CXMED001");

        var di = run.GetSource("GeneratedServiceCollectionExtensions.g.cs");
        Assert.Equal(1, CountOccurrences(di, "typeof(global::TestApp.PingHandler)"));
    }

    [Theory]
    [InlineData("ICommand<int>", "ICommandHandler<Msg, int>",
        "public Task<int> Handle(Msg message, CancellationToken cancellationToken) => Task.FromResult(1);", "CXMED005")]
    [InlineData("ICommand", "ICommandHandler<Msg>",
        "public Task Handle(Msg message, CancellationToken cancellationToken) => Task.CompletedTask;", "CXMED005")]
    [InlineData("IQuery<int>", "IQueryHandler<Msg, int>",
        "public Task<int> Handle(Msg message, CancellationToken cancellationToken) => Task.FromResult(1);", "CXMED006")]
    [InlineData("IStreamQuery<int>", "IStreamQueryHandler<Msg, int>",
        "public async IAsyncEnumerable<int> Handle(Msg message, CancellationToken cancellationToken) { await Task.Yield(); yield return 1; }", "CXMED007")]
    public void DuplicateHandlers_ReportOneTargetedError_WithoutBreakingGeneratedCode(
        string messageInterface, string handlerInterface, string handlerBody, string expectedId)
    {
        var run = GeneratorTestHarness.Run($@"
using Cortex.Mediator.Commands;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{{
    public class Msg : {messageInterface} {{ }}
    public class FirstHandler : {handlerInterface} {{ {handlerBody} }}
    public class SecondHandler : {handlerInterface} {{ {handlerBody} }}
}}");

        var error = Assert.Single(run.GeneratorDiagnostics, d => d.Severity == DiagnosticSeverity.Error);
        Assert.Equal(expectedId, error.Id);
        Assert.Equal("SecondHandler", LocatedText(error));
        Assert.Empty(run.CompilationErrors());
    }

    [Fact]
    public void HandlersHiddenFromGeneratedCode_AreReportedAndSkipped()
    {
        var run = GeneratorTestHarness.Run(@"
using Cortex.Mediator.Commands;
using System.Threading;
using System.Threading.Tasks;

[assembly: Cortex.Mediator.SourceGeneration.CortexMediatorGeneration]

namespace TestApp
{
    public class Visible : ICommand<int> { }
    public class Shared : ICommand<int> { }

    public class Container
    {
        private class Hidden : ICommandHandler<Visible, int>
        {
            public Task<int> Handle(Visible command, CancellationToken cancellationToken) => Task.FromResult(1);
        }

        protected internal class Reachable : ICommandHandler<Shared, int>
        {
            public Task<int> Handle(Shared command, CancellationToken cancellationToken) => Task.FromResult(2);
        }
    }
}");

        AssertSingleDiagnostic(run, "CXMED008", DiagnosticSeverity.Warning, "Hidden");
        Assert.DoesNotContain(run.GeneratorDiagnostics, d => d.Id == "CXMED001");
        Assert.Empty(run.CompilationErrors());

        var di = run.GetSource("GeneratedServiceCollectionExtensions.g.cs");
        Assert.DoesNotContain("Container.Hidden", di);
        Assert.Contains("typeof(global::TestApp.Container.Reachable)", di);
    }

    private static void AssertSingleDiagnostic(
        GeneratorTestHarness.GeneratorRun run, string id, DiagnosticSeverity severity, string locatedAt)
    {
        var matches = run.GeneratorDiagnostics.Where(d => d.Id == id && LocatedText(d) == locatedAt).ToArray();
        Assert.True(matches.Length == 1,
            $"Expected one {id} at '{locatedAt}', got:{Environment.NewLine}{GeneratorTestHarness.Format(run.GeneratorDiagnostics)}");
        Assert.Equal(severity, matches[0].Severity);
    }

    private static string? LocatedText(Diagnostic diagnostic) =>
        diagnostic.Location.IsInSource
            ? diagnostic.Location.SourceTree!.GetText().ToString(diagnostic.Location.SourceSpan)
            : null;

    private static int CountOccurrences(string text, string value)
    {
        var count = 0;
        for (var index = text.IndexOf(value, StringComparison.Ordinal); index >= 0;
             index = text.IndexOf(value, index + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
