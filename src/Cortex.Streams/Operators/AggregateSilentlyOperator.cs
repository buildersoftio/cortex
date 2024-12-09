using Cortex.States;
using Cortex.States.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    public class AggregateSilentlyOperator<TKey, TInput, TAggregate> : IOperator, IStatefulOperator, ITelemetryEnabled
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly Func<TAggregate, TInput, TAggregate> _aggregateFunction;
        private readonly IStateStore<TKey, TAggregate> _stateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        public AggregateSilentlyOperator(Func<TInput, TKey> keySelector, Func<TAggregate, TInput, TAggregate> aggregateFunction, IStateStore<TKey, TAggregate> stateStore)
        {
            _keySelector = keySelector;
            _aggregateFunction = aggregateFunction;
            _stateStore = stateStore;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"aggregate_operator_processed_{typeof(TInput).Name}", "Number of items processed by AggregateOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"aggregate_operator_processing_time_{typeof(TInput).Name}", "Processing time for AggregateOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"AggregateOperator_{typeof(TInput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
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
            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("AggregateOperator.Process"))
                {
                    try
                    {
                        var typedInput = (TInput)input;
                        var key = _keySelector(typedInput);
                        TAggregate aggregate;
                        lock (_stateStore)
                        {
                            aggregate = _stateStore.Get(key);
                            aggregate = _aggregateFunction(aggregate, typedInput);
                            _stateStore.Put(key, aggregate);
                        }
                        span.SetAttribute("key", key.ToString());
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
                var typedInput = (TInput)input;
                var key = _keySelector(typedInput);
                lock (_stateStore)
                {
                    var aggregate = _stateStore.Get(key);
                    aggregate = _aggregateFunction(aggregate, typedInput);
                    _stateStore.Put(key, aggregate);
                }
            }

            // we should not return the value from the state, continue the process further, state is just used to mutate
            // for now we are commenting the next Operator.
            //_nextOperator?.Process(new KeyValuePair<TKey, TAggregate>(key, aggregate));

            // Continue processing
            _nextOperator?.Process(input);
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

        public IEnumerable<IStateStore> GetStateStores()
        {
            yield return _stateStore;
        }
    }
}
