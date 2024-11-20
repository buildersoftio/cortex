using Cortex.Telemetry;

namespace Cortex.Streams.Operators
{
    public interface ITelemetryEnabled
    {
        void SetTelemetryProvider(ITelemetryProvider telemetryProvider);
    }
}
