using System.Diagnostics.Metrics;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryCounter : ICounter
    {
        private readonly Counter<double> _counter;

        public OpenTelemetryCounter(Counter<double> counter)
        {
            _counter = counter;
        }

        public void Increment(double value = 1)
        {
            _counter.Add(value);
        }
    }
}
