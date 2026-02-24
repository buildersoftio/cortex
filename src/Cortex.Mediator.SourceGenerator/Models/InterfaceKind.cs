namespace Cortex.Mediator.SourceGenerator.Models
{
    internal enum InterfaceKind
    {
        ReturningCommand,
        VoidCommand,
        Query,
        StreamQuery,
        Notification,
        PreProcessor,
        PostProcessorWithResponse,
        PostProcessorVoid
    }
}
