using System.Diagnostics.Metrics;

namespace Cortex.Streams.Metrics
{
    public class TelemetryContext
    {
        public bool IsEnabled { get; }

        // Shared Meter instance for all operators
        public Meter Meter { get; set; }

        public TelemetryContext(bool isEnabled)
        {
            IsEnabled = isEnabled;
            if (isEnabled)
            {
                Meter = new Meter("Cortex.Streams", "1.0.0");
            }
        }
    }
}
