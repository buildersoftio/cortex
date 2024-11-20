using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System;

namespace Cortex.Telemetry.OpenTelemetry
{
    public class OpenTelemetryProvider : ITelemetryProvider, IDisposable
    {
        private readonly TracerProvider _tracerProvider;
        private readonly MeterProvider _meterProvider;
        private readonly IMetricsProvider _metricsProvider;
        private readonly ITracingProvider _tracingProvider;


        public OpenTelemetryProvider(ResourceBuilder resourceBuilder,
                                     Action<TracerProviderBuilder> configureTracing = null,
                                     Action<MeterProviderBuilder> configureMetrics = null)
        {
            var tracerBuilder = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resourceBuilder);

            configureTracing?.Invoke(tracerBuilder);
            _tracerProvider = tracerBuilder.Build();

            // Configure MeterProviderBuilder
            var meterBuilder = Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resourceBuilder);

            configureMetrics?.Invoke(meterBuilder);
            _meterProvider = meterBuilder.Build();

            // Initialize providers
            _metricsProvider = new OpenTelemetryMetricsProvider();
            _tracingProvider = new OpenTelemetryTracingProvider();
        }

        public IMetricsProvider GetMetricsProvider()
        {
            return _metricsProvider;
        }

        public ITracingProvider GetTracingProvider()
        {
            return _tracingProvider;
        }

        public void Dispose()
        {
            _tracerProvider?.Dispose();
            _meterProvider?.Dispose();
        }
    }
}
