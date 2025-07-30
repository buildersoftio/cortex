using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Cortex.Vectors
{
    /// <summary>
    /// Fixed‑length bit‑packed vector that implements <see cref="IVector{T}"/> for any IEEE‑754 type <typeparamref name="T"/>.
    /// Each bit encodes 0 → <see cref="T.Zero"/>, 1 → <see cref="T.One"/>.
    /// </summary>
    public sealed class BitVector<T> : IVector<T>, IEquatable<BitVector<T>> where T : IFloatingPointIeee754<T>
    {
        private readonly ulong[] _blocks;

        public int Dimension { get; }
        public int Count => Dimension;

        #region Construction
        public BitVector(int dimension)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dimension);
            Dimension = dimension;
            _blocks = new ulong[(dimension + 63) >> 6];
        }

        /// <summary>Create from indices that should be set to 1.</summary>
        public BitVector(int dimension, IEnumerable<int> oneIndices) : this(dimension)
        {
            foreach (var idx in oneIndices) SetBit(idx, true);
        }

        /// <summary>Create from a span of bools.</summary>
        public BitVector(ReadOnlySpan<bool> bits) : this(bits.Length)
        {
            for (int i = 0; i < bits.Length; i++) if (bits[i]) SetBit(i, true);
        }
        #endregion

        #region Bit helpers
        [MethodImpl(MethodImplOptions.AggressiveInlining)] private static (int blk, int off) Loc(int idx) => (idx >> 6, idx & 63);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBit(int idx)
        {
            if ((uint)idx >= Dimension) throw new IndexOutOfRangeException();
            var (b, o) = Loc(idx);
            return (_blocks[b] & (1UL << o)) != 0UL;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBit(int idx, bool value)
        {
            if ((uint)idx >= Dimension) throw new IndexOutOfRangeException();
            var (b, o) = Loc(idx);
            if (value) _blocks[b] |= 1UL << o; else _blocks[b] &= ~(1UL << o);
        }
        public int PopCount()
        {
            int c = 0; foreach (var v in _blocks) c += BitOperations.PopCount(v); return c;
        }
        #endregion

        #region IVector Implementation
        public T this[int index]
        {
            get => GetBit(index) ? T.One : T.Zero;
        }

        public T Dot(IVector<T> other)
        {
            if (other is BitVector<T> bv)
            {
                ValidateSameDimension(bv);
                int count = 0;
                for (int i = 0; i < _blocks.Length; i++) count += BitOperations.PopCount(_blocks[i] & bv._blocks[i]);
                return T.CreateTruncating(count);
            }
            else
            {
                ValidateSameDimension(other);
                T sum = T.Zero;
                for (int i = 0; i < Dimension; i++) sum += this[i] * other[i];
                return sum;
            }
        }

        public T Norm() => T.Sqrt(T.CreateTruncating(PopCount()));

        public IVector<T> Normalize()
        {
            var n = Norm();
            if (n == T.Zero) throw new InvalidOperationException("Cannot normalize zero bit‑vector.");
            var inv = T.One / n;
            var data = new T[Dimension];
            for (int i = 0; i < Dimension; i++) if (GetBit(i)) data[i] = inv; // zeros already default
            return new DenseVector<T>(data);
        }
        #endregion

        #region IEnumerable
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Dimension; i++) yield return this[i];
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        #endregion

        #region Equality & HashCode
        public bool Equals(BitVector<T>? other)
        {
            if (other is null || other.Dimension != Dimension) return false;
            for (int i = 0; i < _blocks.Length; i++) if (_blocks[i] != other._blocks[i]) return false;
            return true;
        }
        public override bool Equals(object? obj) => obj is BitVector<T> bv && Equals(bv);
        public override int GetHashCode() => HashCode.Combine(Dimension, _blocks.Length > 0 ? _blocks[0] : 0UL);
        #endregion

        private void ValidateSameDimension(IVector<T> other)
        {
            if (other.Dimension != Dimension) throw new ArgumentException("Vector dimensions must match.", nameof(other));
        }
    }
}
