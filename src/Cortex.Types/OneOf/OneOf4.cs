using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Types
{
    /// <summary>
    /// Represents a value that can be one of three specified types
    /// </summary>
    /// <typeparam name="T1">First possible type</typeparam>
    /// <typeparam name="T2">Second possible type</typeparam>
    /// <typeparam name="T3">Third possible type</typeparam>
    /// <typeparam name="T4">Forth possible type</typeparam>
    public readonly struct OneOf<T1, T2, T3, T4> : IEquatable<OneOf<T1, T2, T3, T4>>, IOneOf
    {
        private readonly object _value;
        private readonly int _typeIndex;


        /// <inheritdoc />
        public object Value => _value;

        /// <inheritdoc />
        public int TypeIndex => _typeIndex;

        private OneOf(object value, int typeIndex) =>
            (_value, _typeIndex) = (value, typeIndex);

        public static implicit operator OneOf<T1, T2, T3, T4>(T1 value) => new(value, 0);
        public static implicit operator OneOf<T1, T2, T3, T4>(T2 value) => new(value, 1);
        public static implicit operator OneOf<T1, T2, T3, T4>(T3 value) => new(value, 2);
        public static implicit operator OneOf<T1, T2, T3, T4>(T4 value) => new(value, 3);


        /// <summary>
        /// Checks if the contained value is of or derived from type <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Type to check against</typeparam>
        /// <returns>
        /// True if value is exactly <typeparamref name="T"/> or derived from it
        /// </returns>
        /// <example>
        /// <code>
        /// OneOf&lt;Exception, string&gt; value = new ArgumentException();
        /// value.Is&lt;Exception&gt;(); // true
        /// value.Is&lt;ArgumentException&gt;(); // true
        /// value.Is&lt;string&gt;(); // false
        /// </code>
        /// </example>
        public bool Is<T>() => _value is T;

        /// <summary>
        /// Returns the contained value as <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <exception cref="InvalidCastException">
        /// Thrown when value is not compatible with <typeparamref name="T"/>
        /// </exception>
        /// <example>
        /// <code>
        /// OneOf&lt;int, string&gt; value = "42";
        /// string s = value.As&lt;string&gt;(); // "42"
        /// int i = value.As&lt;int&gt;(); // throws
        /// </code>
        /// </example>
        public T As<T>() => _value is T val ? val : throw new InvalidCastException(GetCastErrorMessage(typeof(T)));


        /// <summary>
        /// Attempts to retrieve the value as <typeparamref name="T"/>
        /// </summary>
        /// <param name="result">Out parameter receiving the value if successful</param>
        /// <returns>True if value is compatible with <typeparamref name="T"/></returns>
        /// <example>
        /// <code>
        /// if (value.TryGet(out string s)) {
        ///     Console.WriteLine(s);
        /// }
        /// </code>
        /// </example>
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
            Func<T4, TResult> t4Handler) => _typeIndex switch
            {
                0 => t1Handler((T1)_value),
                1 => t2Handler((T2)_value),
                2 => t3Handler((T3)_value),
                3 => t4Handler((T4)_value),
                _ => throw new InvalidOperationException("Invalid state")
            };

        /// <summary>
        /// Executes type-specific action with exhaustive case handling
        /// </summary>
        public void Switch(
            Action<T1> t1Action,
            Action<T2> t2Action,
            Action<T3> t3Action,
            Action<T4> t4Action)
        {
            switch (_typeIndex)
            {
                case 0: t1Action((T1)_value); break;
                case 1: t2Action((T2)_value); break;
                case 2: t3Action((T3)_value); break;
                case 3: t4Action((T4)_value); break;
                default: throw new InvalidOperationException("Invalid state");
            }
        }

        private string GetCastErrorMessage(Type targetType) =>
            $"Cannot cast stored type {_value?.GetType().Name ?? "null"} to {targetType.Name}";

        public bool Equals(OneOf<T1, T2, T3, T4> other) =>
                          _typeIndex == other._typeIndex &&
                          Equals(_value, other._value);

        public override bool Equals(object obj) =>
            obj is OneOf<T1, T2> other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(_value, _typeIndex);

        public static bool operator ==(OneOf<T1, T2, T3, T4> left, OneOf<T1, T2, T3, T4> right) =>
            left.Equals(right);

        public static bool operator !=(OneOf<T1, T2, T3, T4> left, OneOf<T1, T2, T3, T4> right) =>
            !left.Equals(right);

        public override string ToString() =>
            _value?.ToString() ?? string.Empty;
    }
}
