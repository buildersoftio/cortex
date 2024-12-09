using Cortex.States;
using Cortex.States.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cortex.Streams.Operators
{
    public class GroupByKeyOperator<TInput, TKey> : IOperator, IStatefulOperator, ITelemetryEnabled
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly IStateStore<TKey, List<TInput>> _stateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        public GroupByKeyOperator(Func<TInput, TKey> keySelector, IStateStore<TKey, List<TInput>> stateStore)
        {
            _keySelector = keySelector;
            _stateStore = stateStore;
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"groupby_operator_processed_{typeof(TInput).Name}", "Number of items processed by GroupByKeyOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"groupby_operator_processing_time_{typeof(TInput).Name}", "Processing time for GroupByKeyOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"GroupByKeyOperator_{typeof(TInput).Name}");

                // Cache delegates
                _incrementProcessedCounter = () => _processedCounter.Increment();
                _recordProcessingTime = value => _processingTimeHistogram.Record(value);
            }
            else
            {
                _incrementProcessedCounter = null;
                _recordProcessingTime = null;
            }

            // Propagate telemetry
            if (_nextOperator is ITelemetryEnabled nextTelemetryEnabled)
            {
                nextTelemetryEnabled.SetTelemetryProvider(_telemetryProvider);
            }
        }

        public void Process(object input)
        {

            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);
            List<TInput> group;

            if (_telemetryProvider != null)
            {
                using (var span = _tracer.StartSpan("GroupByKeyOperator.Process"))
                {
                    var stopwatch = Stopwatch.StartNew();
                    try
                    {

                        lock (_stateStore)
                        {
                            group = _stateStore.Get(key) ?? new List<TInput>();
                            group.Add(typedInput);
                            _stateStore.Put(key, group);
                        }
                        span.SetAttribute("key", key.ToString());
                        span.SetAttribute("group_size", group.Count.ToString());
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
                lock (_stateStore)
                {
                    group = _stateStore.Get(key) ?? new List<TInput>();
                    group.Add(typedInput);
                    _stateStore.Put(key, group);
                }
            }

            _nextOperator?.Process(new KeyValuePair<TKey, List<TInput>>(key, group));
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
