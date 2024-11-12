using Cortex.Streams.Metrics;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    public class SinkOperatorAdapter<TInput> : IOperator
    {
        private readonly ISinkOperator<TInput> _sinkOperator;

        // Telemetry
        private Counter<long> _processedCounter;
        private readonly bool _telemetryEnabled;

        public SinkOperatorAdapter(ISinkOperator<TInput> sinkOperator, TelemetryContext telemetryContext)
        {
            _sinkOperator = sinkOperator;

            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;

            if (_telemetryEnabled)
            {
                var meter = telemetryContext.Meter;
                _processedCounter = meter.CreateCounter<long>(
                    "sink_processed_total",
                    description: "Total number of items processed by the sink operator.");
            }
        }

        public void Process(object input)
        {
            if (_telemetryEnabled)
            {
                _processedCounter.Add(1);
            }

            _sinkOperator.Process((TInput)input);
        }

        public void SetNext(IOperator nextOperator)
        {
            // Sink operator is the end; does nothing
        }
    }
}
