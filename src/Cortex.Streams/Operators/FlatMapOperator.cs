using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// The FlatMapOperator takes each input element, applies a function to produce zero or more output elements, 
    /// and emits each output element individually into the stream.
    /// </summary>
    /// <typeparam name="TInput">The type of the input element.</typeparam>
    /// <typeparam name="TOutput">The type of the output element(s) produced.</typeparam>
    public class FlatMapOperator<TInput, TOutput> : IOperator, IHasNextOperators, ITelemetryEnabled
    {
        private readonly Func<TInput, IEnumerable<TOutput>> _flatMapFunction;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private ICounter _emittedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action _incrementEmittedCounter;
        private Action<double> _recordProcessingTime;

        public FlatMapOperator(Func<TInput, IEnumerable<TOutput>> flatMapFunction)
        {
            _flatMapFunction = flatMapFunction ?? throw new ArgumentNullException(nameof(flatMapFunction));
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metrics = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metrics.CreateCounter($"flatmap_operator_processed_{typeof(TInput).Name}_to_{typeof(TOutput).Name}", "Number of items processed by FlatMapOperator");
                _emittedCounter = metrics.CreateCounter($"flatmap_operator_emitted_{typeof(TInput).Name}_to_{typeof(TOutput).Name}", "Number of items emitted by FlatMapOperator");
                _processingTimeHistogram = metrics.CreateHistogram($"flatmap_operator_processing_time_{typeof(TInput).Name}_to_{typeof(TOutput).Name}", "Processing time for FlatMapOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"FlatMapOperator_{typeof(TInput).Name}_to_{typeof(TOutput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _incrementEmittedCounter = () => _emittedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _incrementEmittedCounter = null;
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
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            if (!(input is TInput typedInput))
                throw new ArgumentException($"Expected input of type {typeof(TInput).Name}, but received {input.GetType().Name}", nameof(input));

            IEnumerable<TOutput> outputs;

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("FlatMapOperator.Process"))
                {
                    try
                    {
                        outputs = _flatMapFunction(typedInput) ?? Array.Empty<TOutput>();
                        span.SetAttribute("status", "success");
                        span.SetAttribute("input_type", typeof(TInput).Name);
                        span.SetAttribute("output_type", typeof(TOutput).Name);
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
                        _recordProcessingTime?.Invoke(stopwatch.Elapsed.TotalMilliseconds);
                        _incrementProcessedCounter?.Invoke();
                    }
                }
            }
            else
            {
                outputs = _flatMapFunction(typedInput) ?? Array.Empty<TOutput>();
            }

            // Emit each output element
            foreach (var output in outputs)
            {
                _incrementEmittedCounter?.Invoke();
                _nextOperator?.Process(output);
            }
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;

            // Propagate telemetry
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled && _telemetryProvider != null)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public IEnumerable<IOperator> GetNextOperators()
        {
            if (_nextOperator != null)
                yield return _nextOperator;
        }
    }
}
