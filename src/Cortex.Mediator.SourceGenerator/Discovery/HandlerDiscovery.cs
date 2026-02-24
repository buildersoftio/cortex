using Cortex.Mediator.SourceGenerator.Models;
using Microsoft.CodeAnalysis;
using System.Collections.Immutable;

namespace Cortex.Mediator.SourceGenerator.Discovery
{
    internal static class HandlerDiscovery
    {
        private static readonly (string InterfaceName, InterfaceKind Kind, int TypeArgCount)[] HandlerInterfaces =
        {
            ("Cortex.Mediator.Commands.ICommandHandler", InterfaceKind.ReturningCommand, 2),
            ("Cortex.Mediator.Commands.ICommandHandler", InterfaceKind.VoidCommand, 1),
            ("Cortex.Mediator.Queries.IQueryHandler", InterfaceKind.Query, 2),
            ("Cortex.Mediator.Streaming.IStreamQueryHandler", InterfaceKind.StreamQuery, 2),
            ("Cortex.Mediator.Notifications.INotificationHandler", InterfaceKind.Notification, 1),
            ("Cortex.Mediator.Processors.IRequestPreProcessor", InterfaceKind.PreProcessor, 1),
            ("Cortex.Mediator.Processors.IRequestPostProcessor", InterfaceKind.PostProcessorWithResponse, 2),
            ("Cortex.Mediator.Processors.IRequestPostProcessor", InterfaceKind.PostProcessorVoid, 1),
        };

        public static ImmutableArray<HandlerRegistration> FindHandlers(INamedTypeSymbol classSymbol)
        {
            var builder = ImmutableArray.CreateBuilder<HandlerRegistration>();

            if (classSymbol.IsAbstractOrInterface() || classSymbol.IsGenericType)
                return builder.ToImmutable();

            foreach (var iface in classSymbol.AllInterfaces)
            {
                if (!iface.IsGenericType)
                    continue;

                var constructed = iface.ConstructedFrom;
                var typeArgCount = constructed.TypeArguments.Length;

                foreach (var (interfaceName, kind, expectedArgCount) in HandlerInterfaces)
                {
                    if (typeArgCount != expectedArgCount)
                        continue;

                    if (!constructed.HasFullyQualifiedMetadataName(interfaceName))
                        continue;

                    var messageType = iface.TypeArguments[0];
                    var resultType = typeArgCount >= 2 ? iface.TypeArguments[1] : null;

                    var registration = new HandlerRegistration(
                        kind: kind,
                        handlerFullyQualifiedName: classSymbol.ToFullyQualifiedString(),
                        messageFullyQualifiedName: messageType.ToFullyQualifiedString(),
                        resultFullyQualifiedName: resultType?.ToFullyQualifiedString(),
                        serviceInterfaceFullyQualifiedName: iface.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat));

                    builder.Add(registration);
                }
            }

            return builder.ToImmutable();
        }
    }
}
