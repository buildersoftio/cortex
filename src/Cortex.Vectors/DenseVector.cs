using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Cortex.Vectors
{
    /// <summary>
    /// Contiguous dense representation of a mathematical vector.
    /// Suitable for small‑to‑medium dimensions (< 10⁶) that are mostly non‑zero.
    /// </summary>
    /// <typeparam name="T">Floating‑point element type.</typeparam>
    public sealed class DenseVector<T> : IVector<T>, IEquatable<DenseVector<T>> where T : IFloatingPointIeee754<T>
    {
        private readonly T[] _data;

        #region Construction

        public DenseVector(ReadOnlySpan<T> span)
        {
            _data = span.ToArray();
        }

        public DenseVector(params T[] values)
        {
            _data = values.Length == 0
                ? throw new ArgumentException("Vector must have at least one component.", nameof(values))
                : (T[])values.Clone();
        }

        /// <summary>Creates a zero‑filled vector of given dimension.</summary>
        public static DenseVector<T> Zeros(int dimension)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimension);
            return new DenseVector<T>(new T[dimension]);
        }

        /// <summary>Creates a vector where every component equals <paramref name="value"/>.</summary>
        public static DenseVector<T> Filled(int dimension, T value)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimension);
            var data = new T[dimension];
            Array.Fill(data, value);
            return new DenseVector<T>(data);
        }

        #endregion

        #region IVector & IReadOnlyList Implementation

        public int Dimension => _data.Length;

        public int Count => _data.Length; // IReadOnlyCollection implementation

        public T this[int index] => _data[index];

        public T Dot(IVector<T> other)
        {
            ValidateSameDimension(other);
            T sum = T.Zero;
            for (int i = 0; i < _data.Length; i++)
                sum += _data[i] * other[i];
            return sum;
        }

        public T Norm()
        {
            T sumSq = T.Zero;
            foreach (var v in _data)
                sumSq += v * v;
            return T.Sqrt(sumSq);
        }

        public IVector<T> Normalize()
        {
            var n = Norm();
            if (n == T.Zero)
                throw new InvalidOperationException("Cannot normalize the zero vector.");
            var scaled = new T[_data.Length];
            for (int i = 0; i < _data.Length; i++)
                scaled[i] = _data[i] / n;
            return new DenseVector<T>(scaled);
        }

        #endregion

        #region Arithmetic Operators

        public static DenseVector<T> operator +(DenseVector<T> left, DenseVector<T> right)
        {
            left.ValidateSameDimension(right);
            var result = new T[left.Dimension];
            for (int i = 0; i < result.Length; i++)
                result[i] = left._data[i] + right._data[i];
            return new DenseVector<T>(result);
        }

        public static DenseVector<T> operator -(DenseVector<T> left, DenseVector<T> right)
        {
            left.ValidateSameDimension(right);
            var result = new T[left.Dimension];
            for (int i = 0; i < result.Length; i++)
                result[i] = left._data[i] - right._data[i];
            return new DenseVector<T>(result);
        }

        public static DenseVector<T> operator *(DenseVector<T> vector, T scalar)
        {
            var result = new T[vector.Dimension];
            for (int i = 0; i < result.Length; i++)
                result[i] = vector._data[i] * scalar;
            return new DenseVector<T>(result);
        }

        public static DenseVector<T> operator *(T scalar, DenseVector<T> vector) => vector * scalar;

        #endregion

        #region Equality & Hash

        public bool Equals(DenseVector<T>? other)
        {
            if (other is null || other.Dimension != Dimension) return false;
            for (int i = 0; i < Dimension; i++)
                if (_data[i] != other._data[i]) return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is DenseVector<T> v && Equals(v);

        public override int GetHashCode() => HashCode.Combine(Dimension, _data[0], _data[^1]);

        #endregion

        #region IEnumerable Implementation

        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)_data).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => _data.GetEnumerator();

        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateSameDimension(IVector<T> other)
        {
            if (other.Dimension != Dimension)
                throw new ArgumentException($"Vector dimensions must match (this: {Dimension}, other: {other.Dimension}).", nameof(other));
        }
    }

}
