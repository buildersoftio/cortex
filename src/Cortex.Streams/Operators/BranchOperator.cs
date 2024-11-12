using Cortex.Streams.Metrics;
using System;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    public class BranchOperator<T> : IOperator
    {
        private readonly string _branchName;
        private readonly IOperator _branchOperator;

        // Telemetry
        private Counter<long> _processedCounter;
        private readonly bool _telemetryEnabled;

        public BranchOperator(string branchName, IOperator branchOperator, TelemetryContext telemetryContext)
        {
            _branchName = branchName;
            _branchOperator = branchOperator;

            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;

            if (_telemetryEnabled)
            {
                var meter = telemetryContext.Meter;
                _processedCounter = meter.CreateCounter<long>(
                    $"branch_{branchName}_processed_total",
                    description: $"Total number of items processed by the branch '{branchName}'.");
            }
        }

        public string BranchName => _branchName;

        public void Process(object input)
        {
            if (_telemetryEnabled)
            {
                _processedCounter.Add(1);
            }

            _branchOperator.Process(input);
        }

        public void SetNext(IOperator nextOperator)
        {
            throw new InvalidOperationException("Cannot set next operator on a BranchOperator.");
        }
    }
}
