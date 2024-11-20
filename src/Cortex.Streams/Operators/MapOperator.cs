using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that transforms data from one type to another.
    /// </summary>
    /// <typeparam name="TInput">The input data type.</typeparam>
    /// <typeparam name="TOutput">The output data type after transformation.</typeparam>
    public class MapOperator<TInput, TOutput> : IOperator, IHasNextOperators, ITelemetryEnabled
    {
        private readonly Func<TInput, TOutput> _mapFunction;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        public MapOperator(Func<TInput, TOutput> mapFunction)
        {
            _mapFunction = mapFunction;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metrics = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metrics.CreateCounter($"map_operator_processed_{typeof(TInput).Name}_to_{typeof(TOutput).Name}", "Number of items processed by MapOperator");
                _processingTimeHistogram = metrics.CreateHistogram($"map_operator_processing_time_{typeof(TInput).Name}_to_{typeof(TOutput).Name}", "Processing time for MapOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"MapOperator_{typeof(TInput).Name}_to_{typeof(TOutput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _recordProcessingTime = null;
            }

            // Propagate telemetry to next operator
            if (_nextOperator is ITelemetryEnabled telemetryEnabled)
            {
                telemetryEnabled.SetTelemetryProvider(telemetryProvider);
            }
        }

        public void Process(object input)
        {
            TOutput output;

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("MapOperator.Process"))
                {
                    try
                    {
                        output = _mapFunction((TInput)input);
                        span.SetAttribute("status", "success");
                    }
                    catch (Exception ex)
                    {
                        span.SetAttribute("status", "error");
                        span.SetAttribute("exception", ex.Message);
                        throw;
                    }
                    finally
                    {
                        stopwatch.Stop();
                        _recordProcessingTime(stopwatch.Elapsed.TotalMilliseconds);
                        _incrementProcessedCounter();
                    }
                }
            }
            else
            {
                output = _mapFunction((TInput)input);
            }

            _nextOperator?.Process(output);
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;

            // Propagate telemetry to next operator
            if (_nextOperator is ITelemetryEnabled telemetryEnabled && _telemetryProvider != null)
            {
                telemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
        }
    }
}
