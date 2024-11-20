using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    public class SourceOperatorAdapter<TOutput> : IOperator, IHasNextOperators, ITelemetryEnabled
    {
        private readonly ISourceOperator<TOutput> _sourceOperator;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _emittedCounter;
        private IHistogram _emissionTimeHistogram;
        private ITracer _tracer;
        private Action _incrementEmittedCounter;
        private Action<double> _recordEmissionTime;

        public SourceOperatorAdapter(ISourceOperator<TOutput> sourceOperator)
        {
            _sourceOperator = sourceOperator;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _emittedCounter = metricsProvider.CreateCounter($"source_operator_emitted_{typeof(TOutput).Name}", "Number of items emitted by SourceOperator");
                _emissionTimeHistogram = metricsProvider.CreateHistogram($"source_operator_emission_time_{typeof(TOutput).Name}", "Emission time for SourceOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"SourceOperator_{typeof(TOutput).Name}");

                // Cache delegates
                _incrementEmittedCounter = () => _emittedCounter.Increment();
                _recordEmissionTime = value => _emissionTimeHistogram.Record(value);
            }
            else
            {
                _incrementEmittedCounter = null;
                _recordEmissionTime = null;
            }

            // Propagate telemetry to the next operator
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public void Process(object input)
        {
            // Not used in source operator
        }

        public void SetNext(IOperator nextOperator)
        {
            _nextOperator = nextOperator;

            // Propagate telemetry to the next operator
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled && _telemetryProvider != null)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }

            // Start the source operator
            Start();
        }

        private void Start()
        {
            _sourceOperator.Start(output =>
            {
                if (_telemetryProvider != null)
                {
                    var stopwatch = Stopwatch.StartNew();

                    using (var span = _tracer.StartSpan("SourceOperator.Emit"))
                    {
                        try
                        {
                            _incrementEmittedCounter();
                            _nextOperator?.Process(output);
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
                            _recordEmissionTime(stopwatch.Elapsed.TotalMilliseconds);
                        }
                    }
                }
                else
                {
                    _nextOperator?.Process(output);
                }
            });
        }

        public void Stop()
        {
            _sourceOperator.Stop();
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
