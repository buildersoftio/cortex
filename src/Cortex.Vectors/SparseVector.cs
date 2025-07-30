using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Cortex.Vectors
{
    public sealed class SparseVector<T> : IVector<T>, IEquatable<SparseVector<T>> where T : IFloatingPointIeee754<T>
    {
        private readonly int _dimension;
        private readonly Dictionary<int, T> _values;

        #region Construction
        public SparseVector(int dimension) : this(dimension, Enumerable.Empty<KeyValuePair<int, T>>()) { }

        public SparseVector(int dimension, IEnumerable<KeyValuePair<int, T>> nonZero)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimension);
            _dimension = dimension;
            _values = new Dictionary<int, T>();
            foreach (var (i, val) in nonZero)
            {
                if (i < 0 || i >= dimension) throw new ArgumentOutOfRangeException(nameof(nonZero), "Index out of range.");
                if (val != T.Zero) _values[i] = val;
            }
        }

        /// <summary>Creates a sparse vector where the provided indices hold the same <paramref name="value"/>.</summary>
        public static SparseVector<T> FromIndices(int dimension, IEnumerable<int> indices, T value)
            => new SparseVector<T>(dimension, indices.Select(i => new KeyValuePair<int, T>(i, value)));
        #endregion

        #region IReadOnlyList Implementation
        public int Dimension => _dimension;
        public int Count => _dimension; // total logical length
        public int NonZeroCount => _values.Count;

        public T this[int index]
        {
            get
            {
                if ((uint)index >= _dimension) throw new IndexOutOfRangeException();
                return _values.TryGetValue(index, out var v) ? v : T.Zero;
            }
        }
        #endregion

        #region Core Vector Operations
        public T Dot(IVector<T> other)
        {
            ValidateSameDimension(other);
            T sum = T.Zero;
            foreach (var (i, v) in _values) sum += v * other[i];
            return sum;
        }

        public T Norm()
        {
            T sumSq = T.Zero;
            foreach (var v in _values.Values) sumSq += v * v;
            return T.Sqrt(sumSq);
        }

        public IVector<T> Normalize()
        {
            var n = Norm();
            if (n == T.Zero) throw new InvalidOperationException("Cannot normalize zero vector.");
            var scaled = _values.Select(kv => new KeyValuePair<int, T>(kv.Key, kv.Value / n));
            return new SparseVector<T>(_dimension, scaled);
        }
        #endregion

        #region Operators
        public static SparseVector<T> operator +(SparseVector<T> a, SparseVector<T> b)
        {
            a.ValidateSameDimension(b);
            var result = new Dictionary<int, T>(a._values);
            foreach (var (i, v) in b._values)
            {
                if (result.TryGetValue(i, out var existing))
                {
                    var sum = existing + v;
                    if (sum == T.Zero) result.Remove(i); else result[i] = sum;
                }
                else result[i] = v;
            }
            return new SparseVector<T>(a._dimension, result);
        }

        public static SparseVector<T> operator -(SparseVector<T> a, SparseVector<T> b)
        {
            a.ValidateSameDimension(b);
            var result = new Dictionary<int, T>(a._values);
            foreach (var (i, v) in b._values)
            {
                if (result.TryGetValue(i, out var existing))
                {
                    var diff = existing - v;
                    if (diff == T.Zero) result.Remove(i); else result[i] = diff;
                }
                else if (v != T.Zero) result[i] = -v;
            }
            return new SparseVector<T>(a._dimension, result);
        }

        public static SparseVector<T> operator *(SparseVector<T> vector, T scalar)
        {
            if (scalar == T.Zero) return new SparseVector<T>(vector._dimension);
            var result = vector._values.ToDictionary(k => k.Key, k => k.Value * scalar);
            return new SparseVector<T>(vector._dimension, result);
        }

        public static SparseVector<T> operator *(T scalar, SparseVector<T> vector) => vector * scalar;
        #endregion

        #region Equality & Hashing
        public bool Equals(SparseVector<T>? other)
        {
            if (other is null || other._dimension != _dimension || other._values.Count != _values.Count) return false;
            foreach (var kv in _values)
                if (!other._values.TryGetValue(kv.Key, out var v) || v != kv.Value) return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is SparseVector<T> sv && Equals(sv);
        public override int GetHashCode() => HashCode.Combine(_dimension, _values.Count);
        #endregion

        #region Enumeration
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < _dimension; i++) yield return this[i];
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateSameDimension(IVector<T> other)
        {
            if (other.Dimension != _dimension) throw new ArgumentException("Vector dimensions must match.", nameof(other));
        }
    }
}
