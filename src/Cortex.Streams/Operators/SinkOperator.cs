using Cortex.Streams.Metrics;
using System;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that consumes data at the end of the stream.
    /// </summary>
    /// <typeparam name="TInput">The type of data consumed by the sink.</typeparam>
    public class SinkOperator<TInput> : IOperator
    {
        private readonly Action<TInput> _sinkFunction;

        // Telemetry
        private Counter<long> _processedCounter;
        private readonly bool _telemetryEnabled;

        public SinkOperator(Action<TInput> sinkFunction, TelemetryContext telemetryContext)
        {
            _sinkFunction = sinkFunction;

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

            _sinkFunction((TInput)input);
        }

        public void SetNext(IOperator nextOperator)
        {
            // Sink operator is the end of the chain; does nothing
        }
    }

}
