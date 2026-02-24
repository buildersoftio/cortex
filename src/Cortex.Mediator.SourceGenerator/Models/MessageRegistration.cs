using System;

namespace Cortex.Mediator.SourceGenerator.Models
{
    internal sealed class MessageRegistration : IEquatable<MessageRegistration>
    {
        public InterfaceKind Kind { get; }
        public string MessageFullyQualifiedName { get; }
        public string ResultFullyQualifiedName { get; }
        public Microsoft.CodeAnalysis.Location Location { get; }

        public MessageRegistration(
            InterfaceKind kind,
            string messageFullyQualifiedName,
            string resultFullyQualifiedName,
            Microsoft.CodeAnalysis.Location location)
        {
            Kind = kind;
            MessageFullyQualifiedName = messageFullyQualifiedName;
            ResultFullyQualifiedName = resultFullyQualifiedName ?? "";
            Location = location;
        }

        public bool Equals(MessageRegistration other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return Kind == other.Kind
                && MessageFullyQualifiedName == other.MessageFullyQualifiedName
                && ResultFullyQualifiedName == other.ResultFullyQualifiedName;
        }

        public override bool Equals(object obj) => Equals(obj as MessageRegistration);

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 31 + Kind.GetHashCode();
                hash = hash * 31 + MessageFullyQualifiedName.GetHashCode();
                hash = hash * 31 + ResultFullyQualifiedName.GetHashCode();
                return hash;
            }
        }
    }
}
