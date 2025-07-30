using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Cortex.Vectors
{
    public static class VectorExtensions
    {
        public static T CosineSimilarity<T>(this IVector<T> a, IVector<T> b) where T : IFloatingPointIeee754<T>
        {
            var denom = a.Norm() * b.Norm();
            return denom == T.Zero ? T.Zero : a.Dot(b) / denom;
        }

        public static SparseVector<T> ToSparse<T>(this DenseVector<T> v) where T : IFloatingPointIeee754<T>
            => new SparseVector<T>(v.Dimension,
                Enumerable.Range(0, v.Dimension)
                          .Where(i => v[i] != T.Zero)
                          .Select(i => new KeyValuePair<int, T>(i, v[i])));
    }
}
