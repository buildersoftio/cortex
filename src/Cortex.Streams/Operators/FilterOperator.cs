using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that filters data based on a predicate.
    /// </summary>
    /// <typeparam name="T">The type of data being filtered.</typeparam>
    public class FilterOperator<T> : IOperator, IHasNextOperators, ITelemetryEnabled
    {
        private readonly Func<T, bool> _predicate;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private ICounter _filteredOutCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;

        private Action _incrementProcessedCounter;
        private Action _incrementFilteredOutCounter;
        private Action<double> _recordProcessingTime;

        public FilterOperator(Func<T, bool> predicate)
        {
            _predicate = predicate;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"filter_operator_processed_{typeof(T).Name}", "Number of items processed by FilterOperator");
                _filteredOutCounter = metricsProvider.CreateCounter($"filter_operator_filtered_out_{typeof(T).Name}", "Number of items filtered out by FilterOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"filter_operator_processing_time_{typeof(T).Name}", "Processing time for FilterOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"FilterOperator_{typeof(T).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _incrementFilteredOutCounter = () => _filteredOutCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _incrementFilteredOutCounter = null;
                _recordProcessingTime = null;
            }

            // Propagate telemetry to the next operator
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public void Process(object input)
        {
            bool isPassed;

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("FilterOperator.Process"))
                {
                    try
                    {
                        isPassed = _predicate((T)input);
                        span.SetAttribute("filter_result", isPassed.ToString());
                        span.SetAttribute("status", "success");
                    }
                    catch (Exception ex)
                    {
                        span.SetAttribute("status", "error");
                        span.SetAttribute("exception", ex.ToString());
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
                isPassed = _predicate((T)input);
            }

            if (isPassed)
            {
                _nextOperator?.Process(input);
            }
            else
            {
                _incrementFilteredOutCounter?.Invoke();
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;

            // Propagate telemetry to the next operator
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled && _telemetryProvider != null)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
            {
                yield return _nextOperator;
            }
        }
    }
}
