# Cortex.Streams.Pulsar 🧠

**Cortex.Streams.Pulsar** is a streaming connector for [Apache Pulsar](https://pulsar.apache.org/), designed to work seamlessly within the **Cortex Data Framework**. It enables real-time data ingestion and publication from/to Pulsar topics, now with full support for **message keys** alongside values.

---

## 🌟 Features

- 🔄 **Pulsar Source Operator**: Consume messages (key + value) from Pulsar topics.
- 🚀 **Pulsar Sink Operator**: Publish messages to Pulsar topics with optional keys.
- 🧩 **Key Support**: Allows key-based partitioning and stream grouping.
- 📦 **Seamless DSL Integration**: Easily compose with other Cortex stream operations.
- ⚡ **Built for Scale**: Backed by Pulsar’s distributed, high-throughput architecture.

---

[![GitHub License](https://img.shields.io/github/license/buildersoftio/cortex)](https://github.com/buildersoftio/cortex/blob/master/LICENSE)
[![NuGet Version](https://img.shields.io/nuget/v/Cortex.Streams.Pulsar?label=Cortex.Streams.Pulsar)](https://www.nuget.org/packages/Cortex.Streams.Pulsar)
[![GitHub contributors](https://img.shields.io/github/contributors/buildersoftio/cortex)](https://github.com/buildersoftio/cortex)
[![Discord Shield](https://discord.com/api/guilds/1310034212371566612/widget.png?style=shield)](https://discord.gg/JnMJV33QHu)


## 🚀 Getting Started

### Install via NuGet

```bash
dotnet add package Cortex.Streams.Pulsar
```

## ✅ Pulsar Sink Operator
In `Program.cs` or `Startup.cs`:
```csharp
using Cortex.Streams;
using Cortex.Streams.Pulsar;

var pulsarSink = new PulsarSinkOperator<string>("pulsar://localhost:6650", "persistent://public/default/input-topic");

var stream = StreamBuilder<string, string>
    .CreateNewStream("PulsarIngester")
    .Stream()
    .Sink(pulsarSink)
    .Build();

stream.Start();

stream.Emit("data1");
stream.Emit("data2");
stream.Emit("data3");
```

## ✅ Pulsar Source Operator

```csharp
using Cortex.Streams;
using Cortex.Streams.Pulsar;

var pulsarSource = new PulsarSourceOperator<string>("pulsar://localhost:6650", "persistent://public/default/input-topic");

var stream = StreamBuilder<string, string>
    .CreateNewStream("PulsarProcessor")
    .Stream(pulsarSource)
    .Map(message => message.ToUpper())
    .Sink(processed => Console.WriteLine($"Processed: {processed}"))
    .Build();

stream.Start();
```

## 🔐 Key Use Cases

- Partition-aware processing using message keys
- Sessionization and user-based aggregations
- Scalable event ingestion pipelines

🧱 Prerequisites
- .NET 6.0 SDK or later
- Apache Pulsar running locally or remotely
- Add Cortex.Streams base package


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
