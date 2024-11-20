using OpenTelemetry.Trace;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryTracingProvider : ITracingProvider
    {
        private readonly Tracer _tracer;

        public OpenTelemetryTracingProvider()
        {
            _tracer = TracerProvider.Default.GetTracer("Cortex.Telemetry");
        }

        public OpenTelemetryTracingProvider(string tracerName)
        {
            _tracer = TracerProvider.Default.GetTracer(tracerName);
        }

        public ITracer GetTracer(string name = null)
        {
            return new OpenTelemetryTracer(name != null ? TracerProvider.Default.GetTracer(name) : _tracer);
        }
    }
}
