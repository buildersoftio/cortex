using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that consumes data at the end of the stream.
    /// </summary>
    /// <typeparam name="TInput">The type of data consumed by the sink.</typeparam>
    public class SinkOperator<TInput> : IOperator, IHasNextOperators, ITelemetryEnabled
    {
        private readonly Action<TInput> _sinkFunction;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        public SinkOperator(Action<TInput> sinkFunction)
        {
            _sinkFunction = sinkFunction;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"sink_operator_processed_{typeof(TInput).Name}", "Number of items processed by SinkOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"sink_operator_processing_time_{typeof(TInput).Name}", "Processing time for SinkOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"SinkOperator_{typeof(TInput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _recordProcessingTime = null;
            }
        }

        public void Process(object input)
        {
            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("SinkOperator.Process"))
                {
                    try
                    {
                        _sinkFunction((TInput)input);
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
                _sinkFunction((TInput)input);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            // Sink operator is terminal; no next operator.
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            yield break;
        }
    }
}
