using System.Collections.Generic;
using System.Numerics;

namespace Cortex.Vectors
{
    /// <summary>
    /// Common contract for vector collections.
    /// Provides dimension metadata and core linear‑algebra operations.
    /// </summary>
    /// <typeparam name="T">Any IEEE‑754 floating‑point numeric type (float, double, Half, decimal).</typeparam>
    public interface IVector<T> : IReadOnlyList<T> where T : IFloatingPointIeee754<T>
    {
        /// <summary>Gets the number of components in the vector.</summary>
        int Dimension { get; }

        /// <summary>Dot (inner) product with another vector.</summary>
        /// <exception cref="ArgumentException">Thrown when vector dimensions differ.</exception>
        T Dot(IVector<T> other);

        /// <summary>Euclidean norm (L2).</summary>
        T Norm();

        /// <summary>Returns a unit‑length copy of this vector.</summary>
        IVector<T> Normalize();
    }
}
