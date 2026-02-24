using Cortex.Mediator.SourceGenerator.Models;
using Microsoft.CodeAnalysis;
using System.Collections.Immutable;

namespace Cortex.Mediator.SourceGenerator.Discovery
{
    internal static class MessageDiscovery
    {
        private static readonly (string InterfaceName, InterfaceKind Kind, bool HasResult)[] MessageInterfaces =
        {
            ("Cortex.Mediator.Commands.ICommand", InterfaceKind.ReturningCommand, true),
            ("Cortex.Mediator.Commands.ICommand", InterfaceKind.VoidCommand, false),
            ("Cortex.Mediator.Queries.IQuery", InterfaceKind.Query, true),
            ("Cortex.Mediator.Streaming.IStreamQuery", InterfaceKind.StreamQuery, true),
            ("Cortex.Mediator.Notifications.INotification", InterfaceKind.Notification, false),
        };

        public static ImmutableArray<MessageRegistration> FindMessages(INamedTypeSymbol classSymbol)
        {
            var builder = ImmutableArray.CreateBuilder<MessageRegistration>();

            if (classSymbol.IsAbstractOrInterface())
                return builder.ToImmutable();

            foreach (var iface in classSymbol.AllInterfaces)
            {
                foreach (var (interfaceName, kind, hasResult) in MessageInterfaces)
                {
                    if (hasResult)
                    {
                        if (!iface.IsGenericType || iface.TypeArguments.Length != 1)
                            continue;

                        if (!iface.ConstructedFrom.HasFullyQualifiedMetadataName(interfaceName))
                            continue;

                        var resultType = iface.TypeArguments[0];
                        var location = classSymbol.Locations.Length > 0
                            ? classSymbol.Locations[0]
                            : Location.None;

                        builder.Add(new MessageRegistration(
                            kind: kind,
                            messageFullyQualifiedName: classSymbol.ToFullyQualifiedString(),
                            resultFullyQualifiedName: resultType.ToFullyQualifiedString(),
                            location: location));
                    }
                    else
                    {
                        // Non-generic interface: ICommand (void) or INotification
                        if (iface.IsGenericType)
                            continue;

                        var ifaceName = iface.ToDisplayString(new SymbolDisplayFormat(
                            globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.Omitted,
                            typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces));

                        if (ifaceName != interfaceName)
                            continue;

                        var location = classSymbol.Locations.Length > 0
                            ? classSymbol.Locations[0]
                            : Location.None;

                        builder.Add(new MessageRegistration(
                            kind: kind,
                            messageFullyQualifiedName: classSymbol.ToFullyQualifiedString(),
                            resultFullyQualifiedName: null,
                            location: location));
                    }
                }
            }

            return builder.ToImmutable();
        }
    }
}
