using Microsoft.CodeAnalysis;

namespace Cortex.Mediator.SourceGenerator.Discovery
{
    internal static class SymbolExtensions
    {
        public static string ToFullyQualifiedString(this ITypeSymbol symbol)
        {
            return symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        }

        public static string ToGlobalPrefixed(this ITypeSymbol symbol)
        {
            var fqn = symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return fqn;
        }

        public static bool HasFullyQualifiedMetadataName(this INamedTypeSymbol symbol, string metadataName)
        {
            var constructed = symbol.ConstructedFrom;
            var name = constructed.ToDisplayString(new SymbolDisplayFormat(
                globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.Omitted,
                typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces,
                genericsOptions: SymbolDisplayGenericsOptions.None));

            return name == metadataName;
        }

        public static bool IsAbstractOrInterface(this INamedTypeSymbol symbol)
        {
            return symbol.IsAbstract || symbol.TypeKind == TypeKind.Interface;
        }

        /// <summary>
        /// True when namespace-level code in the same assembly can name the type: the type and every
        /// containing type are public, internal or protected internal.
        /// </summary>
        public static bool IsAccessibleFromGeneratedCode(this INamedTypeSymbol symbol)
        {
            for (var type = symbol; type != null; type = type.ContainingType)
            {
                switch (type.DeclaredAccessibility)
                {
                    case Accessibility.Public:
                    case Accessibility.Internal:
                    case Accessibility.ProtectedOrInternal:
                        continue;
                    default:
                        return false;
                }
            }

            return true;
        }
    }
}
