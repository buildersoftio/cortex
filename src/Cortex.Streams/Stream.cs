using Cortex.Streams.Metrics;
using Cortex.Streams.Operators;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using System;
using System.Collections.Generic;

namespace Cortex.Streams
{
    /// <summary>
    /// Represents a built stream that can be started and stopped.
    /// </summary>
    /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
    /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
    public class Stream<TIn, TCurrent> : IStream<TIn, TCurrent>
    {
        private readonly string _name;
        private readonly IOperator _operatorChain;
        private readonly List<BranchOperator<TCurrent>> _branchOperators;
        private readonly TelemetryOptions _telemetryOptions;
        private readonly TelemetryContext _telemetryContext;
        private MeterProvider _meterProvider;

        private bool _isStarted;

        internal Stream(string name, IOperator operatorChain, List<BranchOperator<TCurrent>> branchOperators, TelemetryOptions telemetryOptions)
        {
            _name = name;
            _operatorChain = operatorChain;
            _branchOperators = branchOperators;

            _telemetryOptions = telemetryOptions;
        }

        /// <summary>
        /// Starts the stream processing.
        /// </summary>
        public void Start()
        {
            if (_isStarted)
            {
                return;
            }

            _isStarted = true;

            if (_telemetryOptions != null && _telemetryOptions.Enabled)
            {
                InitializeTelemetry();
            }

            if (_operatorChain is SourceOperatorAdapter<TIn> sourceAdapter)
            {
                sourceAdapter.SetNext(null);
            }
        }

        /// <summary>
        /// Stops the stream processing.
        /// </summary>
        public void Stop()
        {
            _isStarted = false;

            if (_operatorChain is SourceOperatorAdapter<TCurrent> sourceAdapter)
            {
                sourceAdapter.Stop();
            }

            _meterProvider?.Dispose();
        }

        /// <summary>
        /// Gets the current status of the stream.
        /// </summary>
        /// <returns>A string indicating whether the stream is running or stopped.</returns>
        public string GetStatus()
        {
            return _isStarted ? "Running" : "Stopped";
        }

        /// <summary>
        /// Emits data into the stream when no source operator is used.
        /// </summary>
        /// <param name="value">The data to emit.</param>
        public void Emit(TIn value)
        {
            if (_isStarted)
            {
                if (_operatorChain is SourceOperatorAdapter<TIn>)
                {
                    throw new InvalidOperationException("Cannot manually emit data to a stream with a source operator.");
                }

                _operatorChain.Process(value);
            }
            else
            {
                throw new InvalidOperationException("Stream has not been started.");
            }
        }

        /// <summary>
        /// Gets the branches in the stream along with their names.
        /// </summary>
        /// <returns>A read-only dictionary of branch names and their corresponding operators.</returns>
        public IReadOnlyDictionary<string, BranchOperator<TCurrent>> GetBranches()
        {
            var branchDict = new Dictionary<string, BranchOperator<TCurrent>>();
            foreach (var branchOperator in _branchOperators)
            {
                branchDict[branchOperator.BranchName] = branchOperator;
            }
            return branchDict;
        }

        private void InitializeTelemetry()
        {
            var meterProviderBuilder = Sdk.CreateMeterProviderBuilder();

            // Set resource attributes if provided
            if (_telemetryOptions?.ResourceBuilder != null)
            {
                meterProviderBuilder.SetResourceBuilder(_telemetryOptions.ResourceBuilder);
            }

            // Add the Meter to the MeterProvider
            meterProviderBuilder.AddMeter("Cortex.Streams");

            // Apply additional configurations
            foreach (var configure in _telemetryOptions?.MeterProviderConfigurations ?? new List<Action<MeterProviderBuilder>>())
            {
                configure(meterProviderBuilder);
            }

            _meterProvider = meterProviderBuilder.Build();
        }
    }
}
