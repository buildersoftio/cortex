using Cortex.States;
using Cortex.States.Operators;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that implements sliding window functionality.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    public class SlidingWindowOperator<TInput, TKey, TWindowOutput> : IOperator, IStatefulOperator, ITelemetryEnabled, IDisposable
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _windowSize;
        private readonly TimeSpan _advanceBy;
        private readonly Func<IEnumerable<TInput>, TWindowOutput> _windowFunction;
        private readonly IStateStore<TKey, List<(TInput input, DateTime timestamp)>> _windowStateStore;
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
        private readonly Timer _timer;
        private readonly object _lock = new object();
        private readonly Dictionary<TKey, List<DateTime>> _windowStartTimes = new Dictionary<TKey, List<DateTime>>();

        public SlidingWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan windowSize,
            TimeSpan advanceBy,
            Func<IEnumerable<TInput>, TWindowOutput> windowFunction,
            IStateStore<TKey, List<(TInput, DateTime)>> windowStateStore,
            IStateStore<(TKey, DateTime), TWindowOutput> windowResultsStateStore = null)
        {
            _keySelector = keySelector;
            _windowSize = windowSize;
            _advanceBy = advanceBy;
            _windowFunction = windowFunction;
            _windowStateStore = windowStateStore;
            _windowResultsStateStore = windowResultsStateStore;

            // Initialize the timer to trigger window evaluations at the smallest interval between window size and advance interval
            var timerInterval = _advanceBy < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : _advanceBy;
            _timer = new Timer(_ => EvaluateWindows(), null, timerInterval, timerInterval);
        }

        private void EvaluateWindows()
        {
            List<(TKey key, DateTime windowStart, List<TInput> windowData)> windowsToProcess = new List<(TKey, DateTime, List<TInput>)>();

            lock (_lock)
            {
                var now = DateTime.UtcNow;
                foreach (var window in _windowStateStore.GetAll())
                {
                    var events = _windowStateStore.Get(window.Key);
                    if (events == null || events.Count == 0)
                        continue;

                    // Initialize window start times for the key if not present
                    if (!_windowStartTimes.ContainsKey(window.Key))
                    {
                        _windowStartTimes[window.Key] = new List<DateTime> { events[0].timestamp };
                    }

                    // Remove expired events
                    events.RemoveAll(e => now - e.timestamp > _windowSize);

                    // Generate new window start times based on advanceBy
                    var windowStarts = _windowStartTimes[window.Key];
                    var latestWindowStart = windowStarts.Last();
                    while (now - latestWindowStart >= _advanceBy)
                    {
                        latestWindowStart = latestWindowStart.Add(_advanceBy);
                        windowStarts.Add(latestWindowStart);
                    }

                    // Evaluate windows
                    foreach (var windowStart in windowStarts.ToList())
                    {
                        if (now - windowStart >= _windowSize)
                        {
                            // Get events within the window
                            var windowData = events
                                .Where(e => e.timestamp >= windowStart && e.timestamp < windowStart + _windowSize)
                                .Select(e => e.input)
                                .ToList();

                            if (windowData.Count > 0)
                            {
                                windowsToProcess.Add((window.Key, windowStart, windowData));
                            }

                            // Remove old window start times
                            windowStarts.Remove(windowStart);
                        }
                    }
                }
            }

            // Process windows outside lock
            foreach (var (key, windowStart, windowData) in windowsToProcess)
            {
                TWindowOutput result;
                if (_telemetryProvider != null)
                {
                    var stopwatch = Stopwatch.StartNew();
                    using (var span = _tracer.StartSpan("SlidingWindowOperator.ProcessWindow"))
                    {
                        try
                        {
                            result = _windowFunction(windowData);
                            span.SetAttribute("key", key.ToString());
                            span.SetAttribute("windowStart", windowStart.ToString());
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
            var timestamp = DateTime.UtcNow;

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();
                using (var span = _tracer.StartSpan("SlidingWindowOperator.Process"))
                {
                    try
                    {
                        lock (_lock)
                        {
                            var events = _windowStateStore.Get(key) ?? new List<(TInput, DateTime)>();
                            events.Add((typedInput, timestamp));
                            _windowStateStore.Put(key, events);
                        }
                        span.SetAttribute("key", key.ToString());
                        span.SetAttribute("timestamp", timestamp.ToString());
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
                    var events = _windowStateStore.Get(key) ?? new List<(TInput, DateTime)>();
                    events.Add((typedInput, timestamp));
                    _windowStateStore.Put(key, events);
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
                _processedCounter = metricsProvider.CreateCounter($"sliding_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by SlidingWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"sliding_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for SlidingWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"SlidingWindowOperator_{typeof(TInput).Name}");

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
