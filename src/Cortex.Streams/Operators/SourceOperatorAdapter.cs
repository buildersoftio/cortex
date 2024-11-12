using Cortex.Streams.Metrics;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    public class SourceOperatorAdapter<TOutput> : IOperator
    {
        private readonly ISourceOperator<TOutput> _sourceOperator;
        private IOperator _nextOperator;

        // Telemetry
        private Counter<long> _emittedCounter;
        private readonly bool _telemetryEnabled;

        public SourceOperatorAdapter(ISourceOperator<TOutput> sourceOperator, TelemetryContext telemetryContext)
        {
            _sourceOperator = sourceOperator;

            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;

            if (_telemetryEnabled)
            {
                var meter = telemetryContext.Meter;
                _emittedCounter = meter.CreateCounter<long>(
                    "source_emitted_total",
                    description: "Total number of items emitted by the source operator.");
            }
        }

        public void Process(object input)
        {
            // Source operator starts the data flow, so Process is not used.
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
            _sourceOperator.Start(output =>
            {
                if (_telemetryEnabled)
                {
                    _emittedCounter.Add(1);
                }
                _nextOperator?.Process(output);
            });
        }

        public void Stop()
        {
            _sourceOperator.Stop();
        }
    }
}
