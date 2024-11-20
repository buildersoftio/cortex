using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryMetricsProvider : IMetricsProvider
    {
        private readonly Meter _meter;
        private readonly ConcurrentDictionary<string, object> _metrics = new();

        public OpenTelemetryMetricsProvider()
        {
            _meter = new Meter("Cortex.Telemetry");
        }

        public OpenTelemetryMetricsProvider(string meterName)
        {
            _meter = new Meter(meterName);
        }

        public ICounter CreateCounter(string name, string description = null)
        {
            var counter = _meter.CreateCounter<double>(name, description: description);
            var wrappedCounter = new OpenTelemetryCounter(counter);
            _metrics[name] = wrappedCounter;
            return wrappedCounter;
        }

       
        public IHistogram CreateHistogram(string name, string description = null)
        {
            var histogram = _meter.CreateHistogram<double>(name, description: description);
            var wrappedHistogram = new OpenTelemetryHistogram(histogram);

            _metrics[name] = wrappedHistogram;
            return wrappedHistogram;
        }
    }
}
