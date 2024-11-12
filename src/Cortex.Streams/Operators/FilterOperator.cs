using Cortex.Streams.Metrics;
using System;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that filters data based on a predicate.
    /// </summary>
    /// <typeparam name="T">The type of data being filtered.</typeparam>
    public class FilterOperator<T> : IOperator
    {
        private readonly Func<T, bool> _predicate;
        private readonly TelemetryContext _telemetryContext;
        private IOperator _nextOperator;


        // Telemetry
        private static readonly Meter Meter = new Meter("Cortex.Streams", "1.0.0");
        private readonly Counter<long> _processedCounter;
        private readonly Counter<long> _passedCounter;
        private readonly Counter<long> _filteredCounter;
        private readonly bool _telemetryEnabled;


        public FilterOperator(Func<T, bool> predicate, TelemetryContext telemetryContext)
        {
            _predicate = predicate;
            _telemetryContext = telemetryContext;
            
            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;
            if (_telemetryEnabled)
            {
                // Initialize counters
                var meter = telemetryContext.Meter;
                _processedCounter = meter.CreateCounter<long>(
                    "filter_processed_total",
                    description: "Total number of items processed by the filter operator.");

                _passedCounter = meter.CreateCounter<long>(
                    "filter_passed_total",
                    description: "Total number of items that passed the filter.");

                _filteredCounter = meter.CreateCounter<long>(
                    "filter_filtered_total",
                    description: "Total number of items filtered out.");
            }
        }

        public void Process(object input)
        {
            T typedInput = (T)input;

            if (_telemetryEnabled)
            {
                _processedCounter.Add(1);
            }

            if (_predicate(typedInput))
            {
                if (_telemetryEnabled)
                {
                    _passedCounter.Add(1);
                }
                _nextOperator?.Process(typedInput);
            }
            else
            {
                if (_telemetryEnabled)
                {
                    _filteredCounter.Add(1);
                }
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;
        }
    }
}
