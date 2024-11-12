using Cortex.Streams.Abstractions;
using Cortex.Streams.Metrics;
using Cortex.Streams.Operators;
using OpenTelemetry.Metrics;
using System.Collections.Generic;

namespace Cortex.Streams
{
    /// <summary>
    /// Builds the final stream after adding a sink.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
    public class SinkBuilder<TIn, TCurrent> : ISinkBuilder<TIn, TCurrent>
    {
        // openTelemetry
        private TelemetryOptions _telemetryOptions;
        private TelemetryContext _telemetryContext;

        private readonly string _name;
        private readonly IOperator _firstOperator;
        private readonly List<BranchOperator<TCurrent>> _branchOperators;


        public SinkBuilder(string name, IOperator firstOperator, List<BranchOperator<TCurrent>> branchOperators, TelemetryOptions telemetryOptions, TelemetryContext telemetryContext)
        {
            _name = name;
            _firstOperator = firstOperator;
            _branchOperators = branchOperators;

            _telemetryOptions = telemetryOptions;
            _telemetryContext = telemetryContext;
        }

        /// <summary>
        /// Builds the stream and returns a stream instance.
        /// </summary>
        /// <returns>A stream instance.</returns>
        public IStream<TIn, TCurrent> Build()
        {
            return new Stream<TIn, TCurrent>(_name, _firstOperator, _branchOperators, _telemetryOptions);
        }

    }
}
