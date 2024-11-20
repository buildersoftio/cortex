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
    /// An operator that implements session window functionality.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TKey">The type of the key to group by.</typeparam>
    /// <typeparam name="TWindowOutput">The type of the output after windowing.</typeparam>
    public class SessionWindowOperator<TInput, TKey, TWindowOutput> : IOperator, IStatefulOperator, ITelemetryEnabled, IDisposable
    {
        private readonly Func<TInput, TKey> _keySelector;
        private readonly TimeSpan _inactivityGap;
        private readonly Func<IEnumerable<TInput>, TWindowOutput> _windowFunction;
        private readonly IStateStore<TKey, SessionWindowState<TInput>> _sessionStateStore;
        private readonly IStateStore<(TKey, DateTime), TWindowOutput> _windowResultsStateStore;
        private IOperator _nextOperator;

        // Telemetry fields
        private ITelemetryProvider _telemetryProvider;
        private ICounter _processedCounter;
        private IHistogram _processingTimeHistogram;
        private ITracer _tracer;
        private Action _incrementProcessedCounter;
        private Action<double> _recordProcessingTime;

        // Timer management
        private readonly Timer _timer;
        private readonly object _lock = new object();

        public SessionWindowOperator(
            Func<TInput, TKey> keySelector,
            TimeSpan inactivityGap,
            Func<IEnumerable<TInput>, TWindowOutput> windowFunction,
            IStateStore<TKey, SessionWindowState<TInput>> sessionStateStore,
            IStateStore<(TKey, DateTime), TWindowOutput> windowResultsStateStore = null)
        {
            _keySelector = keySelector;
            _inactivityGap = inactivityGap;
            _windowFunction = windowFunction;
            _sessionStateStore = sessionStateStore;
            _windowResultsStateStore = windowResultsStateStore;

            // Initialize the timer to periodically check for sessions to close
            _timer = new Timer(_ => CheckForInactiveSessions(), null, _inactivityGap, _inactivityGap);
        }

        private void CheckForInactiveSessions()
        {
            List<(TKey key, SessionWindowState<TInput> session)> sessionsToClose = new List<(TKey, SessionWindowState<TInput>)>();
            var now = DateTime.UtcNow;

            lock (_lock)
            {
                foreach (var item in _sessionStateStore.GetAll())
                {
                    var session = _sessionStateStore.Get(item.Key);
                    if (session == null)
                        continue;

                    if (now - session.LastEventTime >= _inactivityGap)
                    {
                        sessionsToClose.Add((item.Key, session));
                        _sessionStateStore.Remove(item.Key);
                    }
                }
            }

            // Process closed sessions outside the lock
            foreach (var (key, session) in sessionsToClose)
            {
                TWindowOutput result;
                if (_telemetryProvider != null)
                {
                    var stopwatch = Stopwatch.StartNew();
                    using (var span = _tracer.StartSpan("SessionWindowOperator.ProcessSession"))
                    {
                        try
                        {
                            result = _windowFunction(session.Events);
                            span.SetAttribute("key", key.ToString());
                            span.SetAttribute("sessionStart", session.StartTime.ToString());
                            span.SetAttribute("sessionEnd", session.LastEventTime.ToString());
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
                    result = _windowFunction(session.Events);
                }

                // Store the session result in the state store if provided
                if (_windowResultsStateStore != null)
                {
                    _windowResultsStateStore.Put((key, session.StartTime), result);
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
                using (var span = _tracer.StartSpan("SessionWindowOperator.Process"))
                {
                    try
                    {
                        lock (_lock)
                        {
                            var session = _sessionStateStore.Get(key);
                            if (session == null)
                            {
                                session = new SessionWindowState<TInput>
                                {
                                    StartTime = timestamp,
                                    LastEventTime = timestamp,
                                    Events = new List<TInput> { typedInput }
                                };
                                _sessionStateStore.Put(key, session);
                            }
                            else
                            {
                                session.Events.Add(typedInput);
                                session.LastEventTime = timestamp;
                            }
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
                    var session = _sessionStateStore.Get(key);
                    if (session == null)
                    {
                        session = new SessionWindowState<TInput>
                        {
                            StartTime = timestamp,
                            LastEventTime = timestamp,
                            Events = new List<TInput> { typedInput }
                        };
                        _sessionStateStore.Put(key, session);
                    }
                    else
                    {
                        session.Events.Add(typedInput);
                        session.LastEventTime = timestamp;
                    }
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
            yield return _sessionStateStore;
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

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }

    /// <summary>
    /// Represents the state of a session window.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    public class SessionWindowState<TInput>
    {
        public DateTime StartTime { get; set; }
        public DateTime LastEventTime { get; set; }
        public List<TInput> Events { get; set; }
    }
}
