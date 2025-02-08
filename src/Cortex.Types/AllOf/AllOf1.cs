﻿using System;
using System.Diagnostics.CodeAnalysis;

namespace Cortex.Types
{
    /// <summary>
    /// Represents a value that is all of the specified types. The value must be compatible with each of the type parameters.
    /// </summary>
    /// <typeparam name="T1">First required type</typeparam>
    /// <typeparam name="T2">Second required type</typeparam>
    public readonly struct AllOf<T1> : IEquatable<AllOf<T1>>, IAllOf
    {
        private readonly object _value;

        /// <inheritdoc />
        public object Value => _value;

        private AllOf(object value)
        {
            if (!(value is T1))
                throw new ArgumentException($"Value must be compatible with {typeof(T1).Name}.");
            _value = value;
        }

        /// <summary>
        /// Creates an AllOf instance from a value that is compatible with all type parameters.
        /// </summary>
        /// <typeparam name="T">Type of the value which must implement all type parameters</typeparam>
        public static AllOf<T1> Create<T>(T value) where T : T1 => new AllOf<T1>(value);


        // For now we are skipping the implicit operations
        //public static implicit operator AllOf<T1>(T1 value)
        //{
        //    if (value is T1)
        //        return new AllOf<T1>(value);
        //    throw new InvalidCastException($"{typeof(T1).Name} is not compatible.");
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

        public bool Equals(AllOf<T1> other) => Equals(_value, other._value);
        public override bool Equals(object obj) => obj is AllOf<T1> other && Equals(other);
        public override int GetHashCode() => _value?.GetHashCode() ?? 0;

        public static bool operator ==(AllOf<T1> left, AllOf<T1> right) => left.Equals(right);
        public static bool operator !=(AllOf<T1> left, AllOf<T1> right) => !left.Equals(right);

        public override string ToString() => _value?.ToString() ?? string.Empty;
    }
}
