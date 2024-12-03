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
    /// An operator that performs session window aggregation.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TSessionOutput">The type of the output after session windowing.</typeparam>
    public class SessionWindowOperator<TInput, TKey, TSessionOutput> : IOperator, IStatefulOperator, ITelemetryEnabled
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _inactivityGap;
        private readonly Func<IEnumerable<TInput>, TSessionOutput> _sessionFunction;
        private readonly IStateStore<TKey, SessionState<TInput>> _sessionStateStore;
        private readonly IStateStore<SessionKey<TKey>, TSessionOutput> _sessionResultsStateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Timer for checking inactive sessions
        private readonly Timer _sessionExpirationTimer;
        private readonly object _stateLock = new object();

        public SessionWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan inactivityGap,
            Func<IEnumerable<TInput>, TSessionOutput> sessionFunction,
            IStateStore<TKey, SessionState<TInput>> sessionStateStore,
            IStateStore<SessionKey<TKey>, TSessionOutput> sessionResultsStateStore = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _inactivityGap = inactivityGap;
            _sessionFunction = sessionFunction ?? throw new ArgumentNullException(nameof(sessionFunction));
            _sessionStateStore = sessionStateStore ?? throw new ArgumentNullException(nameof(sessionStateStore));
            _sessionResultsStateStore = sessionResultsStateStore;

            // Set up a timer to periodically check for inactive sessions
            _sessionExpirationTimer = new Timer(SessionExpirationCallback, null, inactivityGap, inactivityGap);
        }

        public void SetTelemetryProvider(ITelemetryProvider telemetryProvider)
        {
            _telemetryProvider = telemetryProvider;

            if (_telemetryProvider != null)
            {
                var metricsProvider = _telemetryProvider.GetMetricsProvider();
                _processedCounter = metricsProvider.CreateCounter($"session_window_operator_processed_{typeof(TInput).Name}", "Number of items processed by SessionWindowOperator");
                _processingTimeHistogram = metricsProvider.CreateHistogram($"session_window_operator_processing_time_{typeof(TInput).Name}", "Processing time for SessionWindowOperator");
                _tracer = _telemetryProvider.GetTracingProvider().GetTracer($"SessionWindowOperator_{typeof(TInput).Name}");

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

                using (var span = _tracer.StartSpan("SessionWindowOperator.Process"))
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

            lock (_stateLock)
            {
                SessionState<TInput> sessionState;

                if (!_sessionStateStore.ContainsKey(key))
                {
                    // Start a new session
                    sessionState = new SessionState<TInput>
                    {
                        SessionStartTime = currentTime,
                        LastEventTime = currentTime,
                        Events = new List<TInput> { input }
                    };
                    _sessionStateStore.Put(key, sessionState);
                }
                else
                {
                    sessionState = _sessionStateStore.Get(key);

                    var timeSinceLastEvent = currentTime - sessionState.LastEventTime;

                    if (timeSinceLastEvent <= _inactivityGap)
                    {
                        // Continue the current session
                        sessionState.Events.Add(input);
                        sessionState.LastEventTime = currentTime;
                        _sessionStateStore.Put(key, sessionState);
                    }
                    else
                    {
                        // Session has expired, process it
                        ProcessSession(key, sessionState);

                        // Start a new session
                        sessionState = new SessionState<TInput>
                        {
                            SessionStartTime = currentTime,
                            LastEventTime = currentTime,
                            Events = new List<TInput> { input }
                        };
                        _sessionStateStore.Put(key, sessionState);
                    }
                }
            }
        }

        private void ProcessSession(TKey key, SessionState<TInput> sessionState)
        {
            var sessionOutput = _sessionFunction(sessionState.Events);

            // Optionally store the session result
            if (_sessionResultsStateStore != null)
            {
                var resultKey = new SessionKey<TKey>
                {
                    Key = key,
                    SessionStartTime = sessionState.SessionStartTime,
                    SessionEndTime = sessionState.LastEventTime
                };
                _sessionResultsStateStore.Put(resultKey, sessionOutput);
            }

            // Emit the session output
            _nextOperator?.Process(sessionOutput);

            // Remove the session state
            _sessionStateStore.Remove(key);
        }

        private void SessionExpirationCallback(object state)
        {
            try
            {
                var currentTime = DateTime.UtcNow;
                var keysToProcess = new List<TKey>();

                lock (_stateLock)
                {
                    var allKeys = _sessionStateStore.GetKeys();

                    foreach (var key in allKeys)
                    {
                        var sessionState = _sessionStateStore.Get(key);
                        if (sessionState != null)
                        {
                            var timeSinceLastEvent = currentTime - sessionState.LastEventTime;

                            if (timeSinceLastEvent > _inactivityGap)
                            {
                                // Session has expired
                                keysToProcess.Add(key);
                            }
                        }
                    }
                }

                // Process expired sessions outside the lock
                foreach (var key in keysToProcess)
                {
                    SessionState<TInput> sessionState;

                    lock (_stateLock)
                    {
                        sessionState = _sessionStateStore.Get(key);
                        if (sessionState == null)
                            continue; // Already processed
                    }

                    ProcessSession(key, sessionState);
                }
            }
            catch (Exception ex)
            {
                // Log or handle exceptions as necessary
                Console.WriteLine($"Error in SessionExpirationCallback: {ex.Message}");
            }
        }

        public IEnumerable<IStateStore> GetStateStores()
        {
            yield return _sessionStateStore;
            if (_sessionResultsStateStore != null)
                yield return _sessionResultsStateStore;
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
