using System;
using System.Diagnostics.CodeAnalysis;

namespace Cortex.Types
{
    /// <summary>
    /// Represents a value that is all of the specified types. The value must be compatible with each of the type parameters.
    /// </summary>
    /// <typeparam name="T1">First required type</typeparam>
    /// <typeparam name="T2">Second required type</typeparam>
    public readonly struct AllOf<T1, T2> : IEquatable<AllOf<T1, T2>>, IAllOf
    {
        private readonly object _value;

        /// <inheritdoc />
        public object Value => _value;

        private AllOf(object value)
        {
            if (!(value is T1) || !(value is T2))
                throw new ArgumentException($"Value must be compatible with {typeof(T1).Name} and {typeof(T2).Name}.");
            _value = value;
        }

        /// <summary>
        /// Creates an AllOf instance from a value that is compatible with all type parameters.
        /// </summary>
        /// <typeparam name="T">Type of the value which must implement all type parameters</typeparam>
        public static AllOf<T1, T2> Create<T>(T value) where T : T1, T2 => new AllOf<T1, T2>(value);


        // For now we are skipping the implicit operations
        //public static implicit operator AllOf<T1, T2>(T1 value)
        //{
        //    if (value is T2)
        //        return new AllOf<T1, T2>(value);
        //    throw new InvalidCastException($"{typeof(T1).Name} is not compatible with {typeof(T2).Name}.");
        //}

        //public static implicit operator AllOf<T1, T2>(T2 value)
        //{
        //    if (value is T1)
        //        return new AllOf<T1, T2>(value);
        //    throw new InvalidCastException($"{typeof(T2).Name} is not compatible with {typeof(T1).Name}.");
        //}

        public bool Is<T>() => _value is T;

        public T As<T>()
        {
            if (_value is T val) return val;
            throw new InvalidCastException($"Cannot cast {_value?.GetType().Name} to {typeof(T).Name}.");
        }

        public bool TryGet<T>([NotNullWhen(true)] out T result)
        {
            if (_value is T val)
            {
                result = val;
                return true;
            }
            result = default!;
            return false;
        }

        public bool Equals(AllOf<T1, T2> other) => Equals(_value, other._value);
        public override bool Equals(object obj) => obj is AllOf<T1, T2> other && Equals(other);
        public override int GetHashCode() => _value?.GetHashCode() ?? 0;

        public static bool operator ==(AllOf<T1, T2> left, AllOf<T1, T2> right) => left.Equals(right);
        public static bool operator !=(AllOf<T1, T2> left, AllOf<T1, T2> right) => !left.Equals(right);

        public override string ToString() => _value?.ToString() ?? string.Empty;
    }
}
