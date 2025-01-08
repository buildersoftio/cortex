using Cortex.States;
using Cortex.States.Operators;
using Cortex.Streams.Windows;
using Cortex.Telemetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cortex.Streams.Operators
{
    /// <summary>
    /// An operator that performs tumbling window aggregation.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    public class TumblingWindowOperator<TInput, TKey, TWindowOutput> : IOperator, IStatefulOperator, ITelemetryEnabled
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _windowDuration;
        private readonly Func<IEnumerable<TInput>, TWindowOutput> _windowFunction;
        private readonly IDataStore<TKey, WindowState<TInput>> _windowStateStore;
        private readonly IDataStore<WindowKey<TKey>, TWindowOutput> _windowResultsStateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Timer for window expiration
        private readonly Timer _windowExpirationTimer;
        private readonly object _stateLock = new object();

        public TumblingWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan windowDuration,
            Func<IEnumerable<TInput>, TWindowOutput> windowFunction,
            IDataStore<TKey, WindowState<TInput>> windowStateStore,
            IDataStore<WindowKey<TKey>, TWindowOutput> windowResultsStateStore = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _windowDuration = windowDuration;
            _windowFunction = windowFunction ?? throw new ArgumentNullException(nameof(windowFunction));
            _windowStateStore = windowStateStore ?? throw new ArgumentNullException(nameof(windowStateStore));
            _windowResultsStateStore = windowResultsStateStore;

            // Set up a timer to periodically check for window expirations
            _windowExpirationTimer = new Timer(WindowExpirationCallback, null, _windowDuration, _windowDuration);
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

        public void Process(object input)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            if (!(input is TInput typedInput))
                throw new ArgumentException($"Expected input of type {typeof(TInput).Name}, but received {input.GetType().Name}");

            if (_telemetryProvider != null)
            {
                var stopwatch = Stopwatch.StartNew();

                using (var span = _tracer.StartSpan("TumblingWindowOperator.Process"))
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

            WindowState<TInput> windowState;
            bool isNewWindow = false;

            lock (_stateLock)
            {
                if (!_windowStateStore.ContainsKey(key))
                {
                    // Initialize window state
                    var windowStartTime = GetWindowStartTime(currentTime);
                    windowState = new WindowState<TInput>
                    {
                        WindowStartTime = windowStartTime,
                        Events = new List<TInput>()
                    };
                    _windowStateStore.Put(key, windowState);
                    isNewWindow = true;
                }
                else
                {
                    windowState = _windowStateStore.Get(key);
                }

                // Check if the event falls into the current window
                if (currentTime >= windowState.WindowStartTime && currentTime < windowState.WindowStartTime + _windowDuration)
                {
                    // Event falls into current window
                    windowState.Events.Add(input);
                    _windowStateStore.Put(key, windowState);
                }
                else
                {
                    // Window has closed, process the window
                    ProcessWindow(key, windowState);

                    // Start a new window
                    var newWindowStartTime = GetWindowStartTime(currentTime);
                    windowState = new WindowState<TInput>
                    {
                        WindowStartTime = newWindowStartTime,
                        Events = new List<TInput> { input }
                    };
                    _windowStateStore.Put(key, windowState);
                    isNewWindow = true;
                }
            }

            if (isNewWindow)
            {
                // Optionally, we could set up a timer for this specific key to process the window after the window duration
                // However, since we have a global timer, this might not be necessary
            }
        }

        private void ProcessWindow(TKey key, WindowState<TInput> windowState)
        {
            var windowOutput = _windowFunction(windowState.Events);

            // Optionally store the window result
            if (_windowResultsStateStore != null)
            {
                var resultKey = new WindowKey<TKey>
                {
                    Key = key,
                    WindowStartTime = windowState.WindowStartTime
                };
                _windowResultsStateStore.Put(resultKey, windowOutput);
            }

            // Emit the window output
            _nextOperator?.Process(windowOutput);

            // Remove the window state
            _windowStateStore.Remove(key);
        }

        private void WindowExpirationCallback(object state)
        {
            try
            {
                var currentTime = DateTime.UtcNow;
                var keysToProcess = new List<TKey>();

                lock (_stateLock)
                {
                    var allKeys = _windowStateStore.GetKeys();

                    foreach (var key in allKeys)
                    {
                        var windowState = _windowStateStore.Get(key);
                        if (windowState != null)
                        {
                            if (currentTime >= windowState.WindowStartTime + _windowDuration)
                            {
                                // Window has expired
                                keysToProcess.Add(key);
                            }
                        }
                    }
                }

                // Process expired windows outside the lock to avoid long lock durations
                foreach (var key in keysToProcess)
                {
                    WindowState<TInput> windowState;

                    lock (_stateLock)
                    {
                        windowState = _windowStateStore.Get(key);
                        if (windowState == null)
                            continue; // Already processed
                    }

                    ProcessWindow(key, windowState);
                }
            }
            catch (Exception ex)
            {
                // Log or handle exceptions as necessary
                Console.WriteLine($"Error in WindowExpirationCallback: {ex.Message}");
            }
        }

        private DateTime GetWindowStartTime(DateTime timestamp)
        {
            var windowStartTicks = (long)(timestamp.Ticks / _windowDuration.Ticks) * _windowDuration.Ticks;
            return new DateTime(windowStartTicks, DateTimeKind.Utc);
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
