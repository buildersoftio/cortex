﻿using Cortex.States;
using Cortex.States.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;


namespace Cortex.Streams.Operators
{
    public class AggregateOperator<TKey, TInput, TAggregate> : IOperator, IStatefulOperator, ITelemetryEnabled
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

        public AggregateOperator(Func<TInput, TKey> keySelector, Func<TAggregate, TInput, TAggregate> aggregateFunction, IStateStore<TKey, TAggregate> stateStore)
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
            TAggregate aggregate;
            TKey key;

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("AggregateOperator.Process"))
                {
                    try
                    {
                        var typedInput = (TInput)input;
                        key = _keySelector(typedInput);
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
                key = _keySelector(typedInput);

                lock (_stateStore)
                {
                    aggregate = _stateStore.Get(key);
                    aggregate = _aggregateFunction(aggregate, typedInput);
                    _stateStore.Put(key, aggregate);
                }
            }

            _nextOperator?.Process(new KeyValuePair<TKey, TAggregate>(key, aggregate));
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
