using Cortex.Streams.Abstractions;
using Cortex.Streams.Metrics;
using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams
{
    /// <summary>
    /// Builds a branch within the stream processing pipeline.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the branch.</typeparam>
    public class BranchStreamBuilder<TIn, TCurrent> : IBranchStreamBuilder<TIn, TCurrent>
    {
        // openTelemetry
        private TelemetryOptions _telemetryOptions;
        private TelemetryContext _telemetryContext;


        private readonly string _name;
        internal IOperator _firstOperator;
        internal IOperator _lastOperator;

        public BranchStreamBuilder(string name, TelemetryContext telemetryContext)
        {
            _name = name;
            _telemetryContext = telemetryContext;
        }

        /// <summary>
        /// Adds a filter operator to the branch.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The branch stream builder for method chaining.</returns>
        public IBranchStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate)
        {
            var filterOperator = new FilterOperator<TCurrent>(predicate, _telemetryContext);

            if (_firstOperator == null)
            {
                _firstOperator = filterOperator;
                _lastOperator = filterOperator;
            }
            else
            {
                _lastOperator.SetNext(filterOperator);
                _lastOperator = filterOperator;
            }

            return this;
        }

        /// <summary>
        /// Adds a map operator to the branch to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The branch stream builder with the new data type.</returns>
        public IBranchStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction)
        {
            var mapOperator = new MapOperator<TCurrent, TNext>(mapFunction, _telemetryContext);

            if (_firstOperator == null)
            {
                _firstOperator = mapOperator;
                _lastOperator = mapOperator;
            }
            else
            {
                _lastOperator.SetNext(mapOperator);
                _lastOperator = mapOperator;
            }

            return new BranchStreamBuilder<TIn, TNext>(_name, _telemetryContext)
            {
                _firstOperator = _firstOperator,
                _lastOperator = _lastOperator
            };
        }

        /// <summary>
        /// Adds a sink function to the branch to consume data.
        /// </summary>
        /// <param name="sinkFunction">An action to consume data.</param>
        public void Sink(Action<TCurrent> sinkFunction)
        {
            var sinkOperator = new SinkOperator<TCurrent>(sinkFunction, _telemetryContext);

            if (_firstOperator == null)
            {
                _firstOperator = sinkOperator;
                _lastOperator = sinkOperator;
            }
            else
            {
                _lastOperator.SetNext(sinkOperator);
                _lastOperator = sinkOperator;
            }
        }

        /// <summary>
        /// Adds a sink operator to the branch to consume data.
        /// </summary>
        /// <param name="sinkOperator">A sink operator to consume data.</param>
        public void Sink(ISinkOperator<TCurrent> sinkOperator)
        {
            var sinkAdapter = new SinkOperatorAdapter<TCurrent>(sinkOperator, _telemetryContext);

            if (_firstOperator == null)
            {
                _firstOperator = sinkAdapter;
                _lastOperator = sinkAdapter;
            }
            else
            {
                _lastOperator.SetNext(sinkAdapter);
                _lastOperator = sinkAdapter;
            }
        }
    }
}
