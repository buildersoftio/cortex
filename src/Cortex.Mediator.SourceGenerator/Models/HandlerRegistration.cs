using System;

namespace Cortex.Mediator.SourceGenerator.Models
{
    internal sealed class HandlerRegistration : IEquatable<HandlerRegistration>
    {
        public InterfaceKind Kind { get; }
        public string HandlerFullyQualifiedName { get; }
        public string MessageFullyQualifiedName { get; }
        public string ResultFullyQualifiedName { get; }
        public string ServiceInterfaceFullyQualifiedName { get; }

        public HandlerRegistration(
            InterfaceKind kind,
            string handlerFullyQualifiedName,
            string messageFullyQualifiedName,
            string resultFullyQualifiedName,
            string serviceInterfaceFullyQualifiedName)
        {
            Kind = kind;
            HandlerFullyQualifiedName = handlerFullyQualifiedName;
            MessageFullyQualifiedName = messageFullyQualifiedName;
            ResultFullyQualifiedName = resultFullyQualifiedName ?? "";
            ServiceInterfaceFullyQualifiedName = serviceInterfaceFullyQualifiedName;
        }

        public bool Equals(HandlerRegistration other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return Kind == other.Kind
                && HandlerFullyQualifiedName == other.HandlerFullyQualifiedName
                && MessageFullyQualifiedName == other.MessageFullyQualifiedName
                && ResultFullyQualifiedName == other.ResultFullyQualifiedName
                && ServiceInterfaceFullyQualifiedName == other.ServiceInterfaceFullyQualifiedName;
        }

        public override bool Equals(object obj) => Equals(obj as HandlerRegistration);

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 31 + Kind.GetHashCode();
                hash = hash * 31 + HandlerFullyQualifiedName.GetHashCode();
                hash = hash * 31 + MessageFullyQualifiedName.GetHashCode();
                hash = hash * 31 + ResultFullyQualifiedName.GetHashCode();
                hash = hash * 31 + ServiceInterfaceFullyQualifiedName.GetHashCode();
                return hash;
            }
        }
    }
}
