using Cortex.Streams.Metrics;
using System;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that transforms data from one type to another.
    /// </summary>
    /// <typeparam name="TInput">The input data type.</typeparam>
    /// <typeparam name="TOutput">The output data type after transformation.</typeparam>
    public class MapOperator<TInput, TOutput> : IOperator
    {
        private readonly Func<TInput, TOutput> _mapFunction;
        private IOperator _nextOperator;

        // Telemetry
        private Counter<long> _processedCounter;
        private readonly bool _telemetryEnabled;

        public MapOperator(Func<TInput, TOutput> mapFunction, TelemetryContext telemetryContext)
        {
            _mapFunction = mapFunction;

            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;
            if (_telemetryEnabled)
            {
                var meter = telemetryContext.Meter;
                _processedCounter = meter.CreateCounter<long>(
                    "map_processed_total",
                    description: "Total number of items processed by the map operator.");
            }
        }

        public void Process(object input)
        {
            if (_telemetryEnabled)
            {
                _processedCounter.Add(1);
            }

            var output = _mapFunction((TInput)input);
            _nextOperator?.Process(output);
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
