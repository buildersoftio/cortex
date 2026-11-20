using Cortex.Mediator.SourceGenerator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections.Immutable;
using System.Reflection;
using System.Runtime.Loader;

namespace Cortex.Tests.Mediator.SourceGenerator;

/// <summary>
/// Compiles C# fixtures with the Cortex mediator source generator and, when asked,
/// emits, loads and runs them so generated code is exercised at runtime, not only inspected.
/// </summary>
internal static class GeneratorTestHarness
{
    private const string ScenarioTypeName = "TestApp.Scenario";
    private const string ScenarioMethodName = "Run";

    private static readonly Lazy<MetadataReference[]> References = new(CreateReferences);

    internal sealed record GeneratorRun(
        Compilation OutputCompilation,
        ImmutableArray<Diagnostic> GeneratorDiagnostics,
        ImmutableArray<GeneratedSourceResult> GeneratedSources)
    {
        public string GetSource(string hintName) =>
            GeneratedSources.Single(s => s.HintName == hintName).SourceText.ToString();

        public Diagnostic[] CompilationErrors() =>
            OutputCompilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .ToArray();
    }

    public static GeneratorRun Run(string source)
    {
        var compilation = CSharpCompilation.Create(
            assemblyName: "GeneratorFixture_" + Guid.NewGuid().ToString("N"),
            syntaxTrees: new[] { CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest)) },
            references: References.Value,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new CortexMediatorGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var outputCompilation, out var generatorDiagnostics);

        var result = driver.GetRunResult().Results.Single();
        return new GeneratorRun(outputCompilation, generatorDiagnostics, result.GeneratedSources);
    }

    /// <summary>
    /// Runs the generator, fails the test on any generator or compiler error, then emits and loads the assembly.
    /// </summary>
    public static Assembly CompileAndLoad(string source)
    {
        var run = Run(source);

        var errors = run.GeneratorDiagnostics
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Concat(run.CompilationErrors())
            .ToArray();
        if (errors.Length > 0)
            Assert.Fail("Fixture did not compile:" + Environment.NewLine + Format(errors));

        using var stream = new MemoryStream();
        var emitResult = run.OutputCompilation.Emit(stream);
        if (!emitResult.Success)
            Assert.Fail("Fixture did not emit:" + Environment.NewLine +
                        Format(emitResult.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)));

        stream.Position = 0;
        return AssemblyLoadContext.Default.LoadFromStream(stream);
    }

    /// <summary>
    /// Invokes <c>public static Task&lt;string&gt; TestApp.Scenario.Run()</c> in a loaded fixture and awaits it.
    /// </summary>
    public static async Task<string> InvokeScenarioAsync(Assembly assembly)
    {
        var scenario = assembly.GetType(ScenarioTypeName, throwOnError: true)!;
        var run = scenario.GetMethod(ScenarioMethodName, BindingFlags.Public | BindingFlags.Static)
                  ?? throw new InvalidOperationException(
                      $"Fixture must declare public static Task<string> {ScenarioTypeName}.{ScenarioMethodName}().");

        return await (Task<string>)run.Invoke(null, null)!;
    }

    public static Task<string> RunScenarioAsync(string source) =>
        InvokeScenarioAsync(CompileAndLoad(source));

    public static string Format(IEnumerable<Diagnostic> diagnostics) =>
        string.Join(Environment.NewLine, diagnostics.Select(d => d.ToString()));

    private static MetadataReference[] CreateReferences()
    {
        // Every assembly the test host can load, minus this test assembly: it carries its own copy of the
        // generator's marker attribute, which would clash with the fixture's.
        var trusted = ((string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);
        var testAssemblyPath = typeof(GeneratorTestHarness).Assembly.Location;

        return trusted
            .Where(path => !string.Equals(path, testAssemblyPath, StringComparison.OrdinalIgnoreCase))
            .Select(path => (MetadataReference)MetadataReference.CreateFromFile(path))
            .ToArray();
    }
}
