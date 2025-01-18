using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Windows;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that performs sliding window aggregation.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    public class SlidingWindowOperator<TInput, TKey, TWindowOutput> : IOperator, IStatefulOperator, ITelemetryEnabled
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _windowDuration;
        private readonly TimeSpan _slideInterval;
        private readonly Func<IEnumerable<TInput>, TWindowOutput> _windowFunction;
        private readonly IDataStore<WindowKey<TKey>, List<TInput>> _windowStateStore;
        private readonly IDataStore<WindowKey<TKey>, TWindowOutput> _windowResultsStateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Timer for window processing
        private readonly Timer _windowProcessingTimer;
        private readonly object _stateLock = new object();

        public SlidingWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan windowDuration,
            TimeSpan slideInterval,
            Func<IEnumerable<TInput>, TWindowOutput> windowFunction,
            IDataStore<WindowKey<TKey>, List<TInput>> windowStateStore,
            IDataStore<WindowKey<TKey>, TWindowOutput> windowResultsStateStore = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _windowDuration = windowDuration;
            _slideInterval = slideInterval;
            _windowFunction = windowFunction ?? throw new ArgumentNullException(nameof(windowFunction));
            _windowStateStore = windowStateStore ?? throw new ArgumentNullException(nameof(windowStateStore));
            _windowResultsStateStore = windowResultsStateStore;

            // Set up a timer to periodically process windows
            _windowProcessingTimer = new Timer(WindowProcessingCallback, null, _slideInterval, _slideInterval);
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

        public void Process(object input)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            if (!(input is TInput typedInput))
                throw new ArgumentException($"Expected input of type {typeof(TInput).Name}, but received {input.GetType().Name}");

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("SlidingWindowOperator.Process"))
                {
                    try
                    {
                        ProcessInput(typedInput);
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
                ProcessInput(typedInput);
            }
        }

        private void ProcessInput(TInput input)
        {
            var key = _keySelector(input);
            var currentTime = DateTime.UtcNow;

            var windowStartTimes = GetWindowStartTimes(currentTime);

            lock (_stateLock)
            {
                foreach (var windowStartTime in windowStartTimes)
                {
                    var windowKey = new WindowKey<TKey>
                    {
                        Key = key,
                        WindowStartTime = windowStartTime
                    };

                    List<TInput> windowEvents;
                    if (!_windowStateStore.ContainsKey(windowKey))
                    {
                        windowEvents = new List<TInput>();
                    }
                    else
                    {
                        windowEvents = _windowStateStore.Get(windowKey);
                    }

                    windowEvents.Add(input);
                    _windowStateStore.Put(windowKey, windowEvents);
                }
            }
        }

        private void WindowProcessingCallback(object state)
        {
            try
            {
                var currentTime = DateTime.UtcNow;
                var expiredWindowKeys = new List<WindowKey<TKey>>();

                lock (_stateLock)
                {
                    var allWindowKeys = _windowStateStore.GetKeys();

                    foreach (var windowKey in allWindowKeys)
                    {
                        if (currentTime >= windowKey.WindowStartTime + _windowDuration)
                        {
                            // Window has expired
                            expiredWindowKeys.Add(windowKey);
                        }
                    }
                }

                // Process expired windows outside the lock
                foreach (var windowKey in expiredWindowKeys)
                {
                    List<TInput> windowEvents;

                    lock (_stateLock)
                    {
                        windowEvents = _windowStateStore.Get(windowKey);
                        if (windowEvents == null)
                            continue; // Already processed
                    }

                    ProcessWindow(windowKey, windowEvents);
                }
            }
            catch (Exception ex)
            {
                // Log or handle exceptions as necessary
                Console.WriteLine($"Error in WindowProcessingCallback: {ex.Message}");
            }
        }

        private void ProcessWindow(WindowKey<TKey> windowKey, List<TInput> windowEvents)
        {
            var windowOutput = _windowFunction(windowEvents);

            // Optionally store the window result
            if (_windowResultsStateStore != null)
            {
                _windowResultsStateStore.Put(windowKey, windowOutput);
            }

            // Emit the window output
            _nextOperator?.Process(windowOutput);

            // Remove the window state
            lock (_stateLock)
            {
                _windowStateStore.Remove(windowKey);
            }
        }

        private List<DateTime> GetWindowStartTimes(DateTime eventTime)
        {
            var windowStartTimes = new List<DateTime>();
            var firstWindowStartTime = eventTime - _windowDuration + _slideInterval;
            var windowCount = (int)(_windowDuration.TotalMilliseconds / _slideInterval.TotalMilliseconds);

            for (int i = 0; i < windowCount; i++)
            {
                var windowStartTime = firstWindowStartTime + TimeSpan.FromMilliseconds(i * _slideInterval.TotalMilliseconds);
                if (windowStartTime <= eventTime && eventTime < windowStartTime + _windowDuration)
                {
                    windowStartTimes.Add(windowStartTime);
                }
            }

            return windowStartTimes;
        }

        public IEnumerable<IDataStore> GetStateStores()
        {
            yield return _windowStateStore;
            if (_windowResultsStateStore != null)
                yield return _windowResultsStateStore;
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
    }
}
