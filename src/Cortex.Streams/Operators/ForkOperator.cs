using Cortex.Streams.Metrics;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Cortex.Streams.Operators
{
    internal class ForkOperator<T> : IOperator
    {
        private readonly Dictionary<string, BranchOperator<T>> _branches = new Dictionary<string, BranchOperator<T>>();


        // Telemetry
        private Counter<long> _processedCounter;
        private readonly bool _telemetryEnabled;

        public ForkOperator(TelemetryContext telemetryContext)
        {
            _telemetryEnabled = telemetryContext?.IsEnabled ?? false;

            if (_telemetryEnabled)
            {
                var meter = telemetryContext.Meter;
                _processedCounter = meter.CreateCounter<long>(
                    "fork_processed_total",
                    description: "Total number of items processed by the fork operator.");
            }
        }

        public void AddBranch(string name, BranchOperator<T> branchOperator)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Branch name cannot be null or empty.", nameof(name));
            if (branchOperator == null)
                throw new ArgumentNullException(nameof(branchOperator));

            _branches[name] = branchOperator;
        }

        public void Process(object input)
        {
            if (_telemetryEnabled)
            {
                _processedCounter.Add(1);
            }


            foreach (var branch in _branches.Values)
            {
                branch.Process(input);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            throw new InvalidOperationException("Cannot set next operator on a ForkOperator.");
        }

        public IReadOnlyDictionary<string, BranchOperator<T>> Branches => _branches;
    }
}
