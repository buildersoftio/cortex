using Cortex.States;
using Cortex.States.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that implements tumbling window functionality.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>

    /// <summary>
    /// An operator that implements tumbling window functionality.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    /// <summary>
    /// An operator that implements tumbling window functionality and stores window results.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    public class TumblingWindowOperator<TInput, TKey, TWindowOutput> : IOperator, IStatefulOperator, ITelemetryEnabled, IDisposable
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _windowDuration;
        private readonly Func<IEnumerable<TInput>, TWindowOutput> _windowFunction;
        private readonly IStateStore<TKey, List<TInput>> _windowStateStore;
        private readonly IStateStore<(TKey, DateTime), TWindowOutput> _windowResultsStateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Timer management fields
        private readonly Dictionary<TKey, DateTime> _windowStartTimes = new Dictionary<TKey, DateTime>();
        private readonly Timer _timer;
        private readonly object _lock = new object();

        public TumblingWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan windowDuration,
            Func<IEnumerable<TInput>, TWindowOutput> windowFunction,
            IStateStore<TKey, List<TInput>> windowStateStore,
            IStateStore<(TKey, DateTime), TWindowOutput> windowResultsStateStore = null)
        {
            _keySelector = keySelector;
            _windowDuration = windowDuration;
            _windowFunction = windowFunction;
            _windowStateStore = windowStateStore;
            _windowResultsStateStore = windowResultsStateStore;

            // Initialize the timer
            _timer = new Timer(_ => CloseWindows(), null, _windowDuration, _windowDuration);
        }

        private void CloseWindows()
        {
            List<(TKey key, List<TInput> windowData, DateTime windowStart)> windowsToProcess = new List<(TKey, List<TInput>, DateTime)>();
            lock (_lock)
            {
                var now = DateTime.UtcNow;
                foreach (var kvp in _windowStartTimes)
                {
                    var key = kvp.Key;
                    var windowStart = kvp.Value;
                    if (now - windowStart >= _windowDuration)
                    {
                        var windowData = _windowStateStore.Get(key);
                        if (windowData != null && windowData.Count > 0)
                        {
                            windowsToProcess.Add((key, windowData, windowStart));
                        }

                        // Reset window
                        _windowStateStore.Put(key, new List<TInput>());
                        _windowStartTimes[key] = now;
                    }
                }
            }

            // Process windows outside lock
            foreach (var (key, windowData, windowStart) in windowsToProcess)
            {
                TWindowOutput result;
                if (_telemetryProvider != null)
                {
                    var stopwatch = Stopwatch.StartNew();
                    using (var span = _tracer.StartSpan("TumblingWindowOperator.ProcessWindow"))
                    {
                        try
                        {
                            result = _windowFunction(windowData);
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
                        }
                    }
                }
                else
                {
                    result = _windowFunction(windowData);
                }

                // Store the window result in the state store if provided
                if (_windowResultsStateStore != null)
                {
                    _windowResultsStateStore.Put((key, windowStart), result);
                }

                // Continue processing
                _nextOperator?.Process(result);
            }
        }

        public void Process(object input)
        {
            var typedInput = (TInput)input;
            var key = _keySelector(typedInput);

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("TumblingWindowOperator.Process"))
                {
                    try
                    {
                        lock (_lock)
                        {
                            var windowData = _windowStateStore.Get(key);
                            if (windowData == null)
                            {
                                windowData = new List<TInput>();
                                _windowStateStore.Put(key, windowData);
                                _windowStartTimes[key] = DateTime.UtcNow;
                            }
                            windowData.Add(typedInput);
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
                lock (_lock)
                {
                    var windowData = _windowStateStore.Get(key);
                    if (windowData == null)
                    {
                        windowData = new List<TInput>();
                        _windowStateStore.Put(key, windowData);
                        _windowStartTimes[key] = DateTime.UtcNow;
                    }
                    windowData.Add(typedInput);
                }
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

        public IEnumerable<IStateStore> GetStateStores()
        {
            yield return _windowStateStore;
            if (_windowResultsStateStore != null)
            {
                yield return _windowResultsStateStore;
            }
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"tumbling_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by TumblingWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"tumbling_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for TumblingWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"TumblingWindowOperator_{typeof(TInput).Name}");

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

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
