using System.Diagnostics.Metrics;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryHistogram : IHistogram
    {
        private readonly Histogram<double> _histogram;

        public OpenTelemetryHistogram(Histogram<double> histogram)
        {
            _histogram = histogram;
        }

        public void Record(double value)
        {
            _histogram.Record(value);
        }
    }
}
