using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Cortex.Types
{
    /// <summary>
    /// Represents a value that can be any of the specified types
    /// </summary>
    /// <typeparam name="T1">First possible type</typeparam>
    /// <typeparam name="T2">Second possible type</typeparam>
    /// <typeparam name="T3">Third possible type</typeparam>
    /// <typeparam name="T4">Fourth possible type</typeparam>
    public readonly struct AnyOf<T1, T2, T3, T4> : IEquatable<AnyOf<T1, T2, T3, T4>>, IAnyOf
    {
        private readonly object _value;
        private readonly HashSet<int> _typeIndices;

        /// <inheritdoc />
        public object Value => _value;

        /// <inheritdoc />
        public IEnumerable<int> TypeIndices => _typeIndices;

        private AnyOf(object value, HashSet<int> typeIndices) =>
            (_value, _typeIndices) = (value, typeIndices);

        public static implicit operator AnyOf<T1, T2, T3, T4>(T1 value) =>
            new(value, new HashSet<int> { 0 });

        public static implicit operator AnyOf<T1, T2, T3, T4>(T2 value) =>
            new(value, new HashSet<int> { 1 });

        public static implicit operator AnyOf<T1, T2, T3, T4>(T3 value) =>
            new(value, new HashSet<int> { 2 });

        public static implicit operator AnyOf<T1, T2, T3, T4>(T4 value) =>
            new(value, new HashSet<int> { 3 });

        /// <summary>
        /// Checks if the contained value is of or derived from type <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Type to check against</typeparam>
        /// <returns>
        /// True if value is exactly <typeparamref name="T"/> or derived from it
        /// </returns>
        public bool Is<T>() => _value is T;

        /// <summary>
        /// Returns the contained value as <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <exception cref="InvalidCastException">
        /// Thrown when value is not compatible with <typeparamref name="T"/>
        /// </exception>
        public T As<T>() => _value is T val
            ? val
            : throw new InvalidCastException(GetCastErrorMessage(typeof(T)));

        /// <summary>
        /// Attempts to retrieve the value as <typeparamref name="T"/>
        /// </summary>
        /// <param name="result">Out parameter receiving the value if successful</param>
        /// <returns>True if value is compatible with <typeparamref name="T"/></returns>
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

        /// <summary>
        /// Type-safe pattern matching with exhaustive case handling
        /// </summary>
        public TResult Match<TResult>(
            Func<T1, TResult> t1Handler,
            Func<T2, TResult> t2Handler,
            Func<T3, TResult> t3Handler,
            Func<T4, TResult> t4Handler) => _typeIndices.Contains(0) && _value is T1 t1
            ? t1Handler(t1)
            : _typeIndices.Contains(1) && _value is T2 t2
            ? t2Handler(t2)
            : _typeIndices.Contains(2) && _value is T3 t3
            ? t3Handler(t3)
            : _typeIndices.Contains(3) && _value is T4 t4
            ? t4Handler(t4)
            : throw new InvalidOperationException("Invalid state");

        /// <summary>
        /// Executes type-specific action with exhaustive case handling
        /// </summary>
        public void Switch(
            Action<T1> t1Action,
            Action<T2> t2Action,
            Action<T3> t3Action,
            Action<T4> t4Action)
        {
            if (_typeIndices.Contains(0) && _value is T1 t1)
            {
                t1Action(t1);
            }
            else if (_typeIndices.Contains(1) && _value is T2 t2)
            {
                t2Action(t2);
            }
            else if (_typeIndices.Contains(2) && _value is T3 t3)
            {
                t3Action(t3);
            }
            else if (_typeIndices.Contains(3) && _value is T4 t4)
            {
                t4Action(t4);
            }
            else
            {
                throw new InvalidOperationException("Invalid state");
            }
        }

        /// <summary>
        /// Returns all of the type parameters for which the stored value is assignable.
        /// </summary>
        public IEnumerable<Type> GetMatchingTypes()
        {
            if (_value is T1) yield return typeof(T1);
            if (_value is T2) yield return typeof(T2);
            if (_value is T3) yield return typeof(T3);
            if (_value is T4) yield return typeof(T4);
        }

        private string GetCastErrorMessage(Type targetType) =>
            $"Cannot cast stored type {_value?.GetType().Name ?? "null"} to {targetType.Name}";

        public bool Equals(AnyOf<T1, T2, T3, T4> other) =>
            _typeIndices.SetEquals(other._typeIndices) &&
            Equals(_value, other._value);

        public override bool Equals(object obj) =>
            obj is AnyOf<T1, T2, T3, T4> other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(_value, _typeIndices);

        public static bool operator ==(AnyOf<T1, T2, T3, T4> left, AnyOf<T1, T2, T3, T4> right) =>
            left.Equals(right);

        public static bool operator !=(AnyOf<T1, T2, T3, T4> left, AnyOf<T1, T2, T3, T4> right) =>
            !left.Equals(right);

        public override string ToString() =>
            _value?.ToString() ?? string.Empty;
    }
}
