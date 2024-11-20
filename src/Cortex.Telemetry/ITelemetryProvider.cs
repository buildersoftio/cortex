namespace Cortex.Telemetry
{
    public interface ITelemetryProvider
    {
        IMetricsProvider GetMetricsProvider();
        ITracingProvider GetTracingProvider();
    }
}
