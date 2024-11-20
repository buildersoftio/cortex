# Cortex Data Framework

**Cortex Data Framework** is a robust, extensible platform designed to facilitate real-time data streaming, processing, and state management. It provides developers with a comprehensive suite of tools and libraries to build scalable, high-performance data pipelines tailored to diverse use cases. By abstracting underlying streaming technologies and state management solutions, Cortex Data Framework enables seamless integration, simplified development workflows, and enhanced maintainability for complex data-driven applications.

## Key Features
- **Modular Architecture**: Comprises distinct, interchangeable modules for streaming, state management, and connectors, allowing developers to choose components that best fit their requirements.

- **Extensive Streaming Support**: Natively integrates with popular streaming platforms like Kafka and Pulsar, ensuring reliable and efficient data ingestion and distribution.

- **Flexible State Management**: Offers in-memory and persistent state storage options (e.g., RocksDB) to maintain stateful computations and enable advanced analytics.

- **Developer-Friendly APIs**: Provides intuitive and expressive APIs for building, configuring, and managing data streams and state stores, reducing development overhead.

- **Thread-Safe Operations**: Ensures data integrity and consistency through built-in thread safety mechanisms, suitable for concurrent processing environments.

- **Telemetry and Monitoring**: Integrates with telemetry tools to monitor performance metrics, aiding in proactive maintenance and optimization.


## Use Cases

- Real-time analytics and monitoring
- Event-driven architectures
- Stateful stream processing (e.g., aggregations, joins)
- Data enrichment and transformation pipelines
- Scalable data ingestion and distribution systems

## Features

- **Modular Architecture:** Distinct, interchangeable modules for streaming, state management, and connectors.
- **Extensive Streaming Support:** Integrates with popular streaming platforms like Kafka and Pulsar.
- **Flexible State Management:** In-memory and persistent state storage options (e.g., RocksDB).
- **Developer-Friendly APIs:** Intuitive and expressive APIs for building, configuring, and managing data streams.
- **Thread-Safe Operations:** Ensures data integrity and consistency in concurrent processing environments.
- **Telemetry and Monitoring:** Integrates with telemetry tools to monitor performance metrics.

## Project Structure

- **Cortex.Streams:** Core streaming capabilities for building data pipelines.
- **Cortex.Streams.Kafka:** Integration with Apache Kafka for robust data streaming.
- **Cortex.Streams.Pulsar:** Integration with Apache Pulsar for versatile messaging needs.
- **Cortex.Streams.RabbitMQ:** Integration with RabbitMQ for versatile messaging needs.
- **Cortex.Streams.AWSSQS:** Integration with Amazon SQS for messaging needs in the cloud.
- **Cortex.Streams.AzureServiceBus:** Integration with Azure Messaging Service Bus for messaging needs in the cloud.
- **Cortex.Streams.AzureBlobStorage:** Integration with Azure Blob Storage for sinking messages.
- **Cortex.Streams.S3:** Integration with AWS S3 for sinking messages.
- **Cortex.States:** Core state management functionalities.
- **Cortex.States.RocksDb:** Persistent state storage using RocksDB.
- **Cortex.Telemetry:** Core library to add support for Tracing and Matrics.
- **Cortex.Telemetry.OpenTelemetry:** Adds support for Open Telemetry.

## Getting Started

### Prerequisites

- **.NET 6.0 SDK** or later
- **NuGet Package Manager** (integrated with Visual Studio or available via CLI)
- **Apache Kafka** (if using Cortex.Streams.Kafka)
- **Apache Pulsar** (if using Cortex.Streams.Pulsar)

### Installation

Cortex Data Framework is available through the NuGet Package Manager. You can easily add the necessary packages to your .NET project using the following methods:

#### Using the .NET CLI

Open your terminal or command prompt and navigate to your project directory, then run the following commands to install the desired packages:

```bash
# Install Cortex.Streams
dotnet add package Cortex.Streams

# Install Cortex.States
dotnet add package Cortex.States
```

#### Using the Package Manager Console in Visual Studio
1. Open your project in Visual Studio.
2. Navigate to **Tools > NuGet Package Manager > Package Manager Console**.
3. Run the following commands:

```powershell
# Install Cortex.Streams
Install-Package Cortex.Streams

# Install Cortex.States
Install-Package Cortex.States
```

## Usage

Cortex Data Framework makes it easy to set up and run real-time data processing pipelines. Below are some simple examples to get you started.



### 1. Real-Time Click Tracking

**Scenario**: Track the number of clicks on different web pages in real-time and display the aggregated counts.

Steps:

**1. Define the Event Class**

```csharp
public class ClickEvent
{
    public string PageUrl { get; set; }
    public DateTime Timestamp { get; set; }
}
```

**2. Build the Stream Pipeline**

- **Stream**: Starts with the source operator.
- **Filter**: Filters events based on certain criteria.
- **GroupBy**: Groups events by PageUrl.
- **Aggregate**: Counts the number of clicks per page.
- **Sink**: Prints the total clicks per page.

```csharp
        static void Main(string[] args)
        {
            // Build the stream
            var stream = StreamBuilder<ClickEvent, ClickEvent>.CreateNewStream("ClickStream")
                .Stream()
                .Filter(e => !string.IsNullOrEmpty(e.PageUrl))
                .GroupBy(
                    e => e.PageUrl,                   // Key selector: group by PageUrl
                    stateStoreName: "ClickGroupStore")
                .Aggregate<string, int>(
                    e => e.PageUrl,             // Key selector for aggregation
                    (count, e) => count + 1,          // Aggregation function: increment count
                    stateStoreName: "ClickAggregateStore")
                .Sink(e =>
                {
                    Console.WriteLine($"Page: {e.PageUrl}");
                })
                .Build();

            // start the stream
            stream.Start();
```

Emitting some random events into the stream

```csharp
// emit some events

var random = new Random();
var pages = new[] { "/home", "/about", "/contact", "/products" };

while (true)
{
    var page = pages[random.Next(pages.Length)];
    var click = new ClickEvent
    {
        PageUrl = page,
        Timestamp = DateTime.UtcNow
    };

    stream.Emit(click);
    Thread.Sleep(100); // Simulate click rate
}
```

**3. Access Aggregated Data**

Retrieve and display the click counts from the state store

```csharp
// Access the aggregate state store data
var aggregateStore = stream.GetStateStoreByName<InMemoryStateStore<string, int>>("ClickAggregateStore");

// Access the groupby state store data
var groupByStore = stream.GetStateStoreByName<InMemoryStateStore<string, List<ClickEvent>>>("ClickGroupStore")


if (aggregateStore != null)
{
    Console.WriteLine("\nAggregated Click Counts:");
    foreach (var kvp in aggregateStore.GetAll())
    {
        Console.WriteLine($"Page: {kvp.Key}, Clicks: {kvp.Value}");
    }
}
else
{
    Console.WriteLine("Aggregate state store not found.");
}
```


## Contributing
We welcome contributions from the community! Whether it's reporting bugs, suggesting features, or submitting pull requests, your involvement helps improve Cortex for everyone.

### How to Contribute
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

## License
This project is licensed under the MIT License.

## Sponsorship
Cortex is an open-source project maintained by BuilderSoft. Your support helps us continue developing and improving Vortex. Consider sponsoring us to contribute to the future of resilient streaming platforms.

### How to Sponsor
* **Financial Contributions**: Support us through [GitHub Sponsors](https://github.com/sponsors/buildersoftio) or other preferred platforms.
* **Corporate Sponsorship**: If your organization is interested in sponsoring Vortex, please contact us directly.

Contact Us: cortex@buildersoft.io


## Contact
We'd love to hear from you! Whether you have questions, feedback, or need support, feel free to reach out.

- Email: cortex@buildersoft.io
- Website: https://buildersoft.io
- GitHub Issues: [Cortex Data Framework Issues](https://github.com/buildersoftio/cortex/issues)


Thank you for using Cortex Data Framework! We hope it empowers you to build scalable and efficient data processing pipelines effortlessly.

Built with ❤️ by the Buildersoft team.