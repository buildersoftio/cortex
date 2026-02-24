using Microsoft.CodeAnalysis;

namespace Cortex.Mediator.SourceGenerator.Diagnostics
{
    internal static class DiagnosticDescriptors
    {
        public static readonly DiagnosticDescriptor CommandHasNoHandler = new(
            id: "CXMED001",
            title: "Command has no handler",
            messageFormat: "Command '{0}' has no registered ICommandHandler<{0}, TResult>",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor QueryHasNoHandler = new(
            id: "CXMED002",
            title: "Query has no handler",
            messageFormat: "Query '{0}' has no registered IQueryHandler<{0}, TResult>",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor StreamQueryHasNoHandler = new(
            id: "CXMED003",
            title: "StreamQuery has no handler",
            messageFormat: "StreamQuery '{0}' has no registered IStreamQueryHandler<{0}, TResult>",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor NotificationHasNoHandler = new(
            id: "CXMED004",
            title: "Notification has no handler",
            messageFormat: "Notification '{0}' has no registered INotificationHandler<{0}>",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Info,
            isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MultipleHandlersForCommand = new(
            id: "CXMED005",
            title: "Multiple handlers for same command",
            messageFormat: "Command '{0}' has multiple handlers: {1}. Only one handler per command is allowed.",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        public static readonly DiagnosticDescriptor MultipleHandlersForQuery = new(
            id: "CXMED006",
            title: "Multiple handlers for same query",
            messageFormat: "Query '{0}' has multiple handlers: {1}. Only one handler per query is allowed.",
            category: "Cortex.Mediator",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);
    }
}
