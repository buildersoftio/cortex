# Cortex.Vectors 🧠

**Cortex.Vectors** is a High‑performance vector types—Dense, Sparse, and Bit—for AI & for .NET.


Built as part of the [Cortex Data Framework](https://github.com/buildersoftio/cortex), this library offers High‑performance vector types—Dense, Sparse, and Bit—for AI for:


- ✨ Generic‑math powered (IFloatingPointIeee754<T>): works with float, double, decimal, …
- 🟢 DenseVector – contiguous storage, SIMD‑friendly operations
- 🔵 SparseVector – dictionary‑backed, memory‑efficient for huge, mostly‑zero spaces
- 🟡 BitVector – bit‑packed booleans with popcount & logical ops
- ⚙️ Core ops out‑of‑the‑box: dot product, L2 norm, cosine similarity, scaling, +/‑

---

[![GitHub License](https://img.shields.io/github/license/buildersoftio/cortex)](https://github.com/buildersoftio/cortex/blob/master/LICENSE)
[![NuGet Version](https://img.shields.io/nuget/v/Cortex.Vectors?label=Cortex.Vectors)](https://www.nuget.org/packages/Cortex.Vectors)
[![GitHub contributors](https://img.shields.io/github/contributors/buildersoftio/cortex)](https://github.com/buildersoftio/cortex)
[![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


## 🚀 Getting Started

### Install via NuGet

```bash
dotnet add package Cortex.Vectors
```

## DenseVector
```csharp
using Cortex.Vectors;

// (1, 2, 3)
var a = new DenseVector<float>(1f, 2f, 3f);

// (0.5, 0.5, 0.5)
var b = DenseVector<float>.Filled(3, 0.5f);

float dot       = a.Dot(b);          // = 3.0
var   normA     = a.Norm();          // ≈ 3.7417
var   unitA     = a.Normalize();     // unit length
float cosine    = a.CosineSimilarity(b);
```

## SparseVector
```csharp
using Cortex.Vectors;
using System.Collections.Generic;

// 1‑million‑dimensional vector with two non‑zeros
var sv = new SparseVector<double>(
    dimension: 1_000_000,
    nonZero: new[]
    {
        new KeyValuePair<int,double>(42,      1.0),
        new KeyValuePair<int,double>(123456,  2.5)
    });

double l2   = sv.Norm();        // √(1² + 2.5²)
var    unit = sv.Normalize();
```

## BitVector

```csharp
using Cortex.Vectors;

// length 128, bits 0, 3, and 5 set to 1
var bv = new BitVector<float>(128, new[] { 0, 3, 5 });

int   ones   = bv.PopCount();   // 3
float selfDot = bv.Dot(bv);     // 3.0 (generic type ⇒ float)
var   l2      = bv.Norm();      // √3
```

## 💬 Contributing
We welcome contributions from the community! Whether it's reporting bugs, suggesting features, or submitting pull requests, your involvement helps improve Cortex for everyone.

### 💬 How to Contribute
1. **Fork the Repository**
2. **Create a Feature Branch**
```bash
git checkout -b feature/YourFeature
```
3. **Commit Your Changes**
```bash
git commit -m "Add your feature"
```
4. **Push to Your Fork**
```bash
git push origin feature/YourFeature
```
5. **Open a Pull Request**

Describe your changes and submit the pull request for review.

## 📄 License
This project is licensed under the MIT License.

## 📚 Sponsorship
Cortex is an open-source project maintained by BuilderSoft. Your support helps us continue developing and improving Cortex. Consider sponsoring us to contribute to the future of resilient streaming platforms.

### How to Sponsor
* **Financial Contributions**: Support us through [GitHub Sponsors](https://github.com/sponsors/buildersoftio) or other preferred platforms.
* **Corporate Sponsorship**: If your organization is interested in sponsoring Cortex, please contact us directly.

Contact Us: cortex@buildersoft.io


## Contact
We'd love to hear from you! Whether you have questions, feedback, or need support, feel free to reach out.

- Email: cortex@buildersoft.io
- Website: https://buildersoft.io
- GitHub Issues: [Cortex Data Framework Issues](https://github.com/buildersoftio/cortex/issues)
- Join our Discord Community: [![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


Thank you for using Cortex Data Framework! We hope it empowers you to build scalable and efficient data processing pipelines effortlessly.

Built with ❤️ by the Buildersoft team.
