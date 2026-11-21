using Cortex.Streams;
using Cortex.Streams.ErrorHandling;
using Cortex.Streams.Operators;
using System.Collections.Concurrent;

namespace Cortex.Streams.Tests
{
    /// <summary>
    /// Tests for error handling with source operators.
    /// These tests verify that:
    /// - Source operators properly propagate error handling to downstream operators
    /// - Skip strategy allows source operators to continue emitting after downstream errors
    /// - Stop strategy properly stops source operators when errors occur
    /// - Retry strategy works with source operators
    /// - Source operators don't hang or loop when errors occur
    /// </summary>
    public class SourceOperatorErrorHandlingTests
    {
        #region Mock Source Operator

        /// <summary>
        /// A test source operator that emits values on demand and tracks its state.
        /// </summary>
        private class TestSourceOperator<T> : ISourceOperator<T>
        {
            private Action<T> _emit;
            private bool _isRunning;
            private readonly ConcurrentQueue<T> _pendingItems = new();
            private readonly ManualResetEventSlim _emitEvent = new(false);
            private Task _emitTask;
            private CancellationTokenSource _cts;

            public bool IsRunning => _isRunning;
            public int EmitCount { get; private set; }
            public bool WasStoppedExplicitly { get; private set; }

            public void Start(Action<T> emit)
            {
                _emit = emit ?? throw new ArgumentNullException(nameof(emit));
                _isRunning = true;
                _cts = new CancellationTokenSource();

                // Process pending items in background
                _emitTask = Task.Run(() =>
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        _emitEvent.Wait(_cts.Token);
                        _emitEvent.Reset();

                        while (_pendingItems.TryDequeue(out var item) && !_cts.Token.IsCancellationRequested)
                        {
                            try
                            {
                                _emit(item);
                                EmitCount++;
                            }
                            catch (Exception)
                            {
                                // Source operators typically catch exceptions from emit
                                // to prevent the consume loop from crashing
                            }
                        }
                    }
                }, _cts.Token);
            }

            public void Stop()
            {
                WasStoppedExplicitly = true;
                _isRunning = false;
                _cts?.Cancel();
                _emitEvent.Set(); // Wake up the task to exit
            }

            /// <summary>
            /// Enqueue an item to be emitted by the source operator.
            /// </summary>
            public void EnqueueItem(T item)
            {
                _pendingItems.Enqueue(item);
                _emitEvent.Set();
            }

            /// <summary>
            /// Wait for all pending items to be processed.
            /// </summary>
            public async Task WaitForProcessingAsync(TimeSpan timeout)
            {
                var deadline = DateTime.UtcNow + timeout;
                while (_pendingItems.Count > 0 && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(10);
                }
            }
        }

        /// <summary>
        /// A simpler synchronous test source operator for immediate testing.
        /// </summary>
        private class SynchronousTestSourceOperator<T> : ISourceOperator<T>
        {
            private Action<T> _emit;
            public bool IsRunning { get; private set; }
            public bool WasStoppedExplicitly { get; private set; }
            public int EmitCount { get; private set; }

            public void Start(Action<T> emit)
            {
                _emit = emit;
                IsRunning = true;
            }

            public void Stop()
            {
                WasStoppedExplicitly = true;
                IsRunning = false;
            }

            /// <summary>
            /// Emit an item synchronously through the source operator.
            /// </summary>
            public void EmitItem(T item)
            {
                _emit(item);
                EmitCount++;
            }
        }

        #endregion

        #region Skip Strategy Tests

        [Fact]
        public void SourceOperator_WithSkipStrategy_ContinuesAfterDownstreamError()
        {
            // Arrange
            var processedItems = new List<int>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Skip
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceSkipTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    if (x == 2) throw new InvalidOperationException("Error on item 2");
                    return x * 10;
                })
                .Sink(x => processedItems.Add(x))
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);
            sourceOperator.EmitItem(2); // This should be skipped
            sourceOperator.EmitItem(3);
            sourceOperator.EmitItem(4);

            // Assert - the error on item 2 did not stop the source
            Assert.True(sourceOperator.IsRunning);
            Assert.False(sourceOperator.WasStoppedExplicitly);

            stream.Stop();

            Assert.Equal(new[] { 10, 30, 40 }, processedItems);
            Assert.Equal(4, sourceOperator.EmitCount); // All items were emitted
            Assert.True(sourceOperator.WasStoppedExplicitly); // Stopped by stream.Stop()
        }

        [Fact]
        public void SourceOperator_WithSkipStrategy_SkipsSinkErrors()
        {
            // Arrange
            var processedItems = new List<int>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Skip
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceSkipSinkTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x => x * 10)
                .Sink(x =>
                {
                    if (x == 20) throw new InvalidOperationException("Sink error on 20");
                    processedItems.Add(x);
                })
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);
            sourceOperator.EmitItem(2); // Sink will error on 20
            sourceOperator.EmitItem(3);

            stream.Stop();

            // Assert
            Assert.Equal(new[] { 10, 30 }, processedItems);
        }

        #endregion

        #region Stop Strategy Tests

        [Fact]
        public void SourceOperator_WithStopStrategy_StopsAfterError()
        {
            // Arrange
            var processedItems = new List<int>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Stop
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceStopTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    if (x == 2) throw new InvalidOperationException("Error on item 2");
                    return x * 10;
                })
                .Sink(x => processedItems.Add(x))
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);
            sourceOperator.EmitItem(2); // This should trigger stop

            // Assert
            Assert.Equal(new[] { 10 }, processedItems);
            Assert.True(sourceOperator.WasStoppedExplicitly);
        }

        #endregion

        #region Retry Strategy Tests

        [Fact]
        public void SourceOperator_WithRetryStrategy_RetriesAndSucceeds()
        {
            // Arrange
            var attemptCount = 0;
            var processedItems = new List<int>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Retry,
                MaxRetries = 3,
                RetryDelay = TimeSpan.Zero
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceRetryTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    attemptCount++;
                    if (attemptCount < 3 && x == 1)
                        throw new InvalidOperationException("Transient error");
                    return x * 10;
                })
                .Sink(x => processedItems.Add(x))
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);

            stream.Stop();

            // Assert
            Assert.Equal(3, attemptCount);
            Assert.Equal(new[] { 10 }, processedItems);
        }

        [Fact]
        public void SourceOperator_WithRetryStrategy_StopsWhenMaxRetriesExceeded()
        {
            // Arrange
            var attemptCount = 0;
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Retry,
                MaxRetries = 2,
                RetryDelay = TimeSpan.Zero
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceRetryExceededTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    attemptCount++;
                    throw new InvalidOperationException("Always fails");
#pragma warning disable CS0162
                    return x;
#pragma warning restore CS0162
                })
                .Sink(x => { })
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);

            // Assert - Source should be stopped after max retries exceeded
            Assert.True(sourceOperator.WasStoppedExplicitly);
            Assert.Equal(2, attemptCount);
        }

        [Fact]
        public void SourceOperator_WithCustomErrorHandler_IsNotCalledAgainForDownstreamStop()
        {
            // Arrange
            var handlerCalls = new List<Exception>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                OnError = ctx =>
                {
                    handlerCalls.Add(ctx.Exception);
                    return ErrorHandlingDecision.Stop;
                }
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceStopNotRedecidedTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map<int>(x => throw new InvalidOperationException("Map failed"))
                .Sink(x => { })
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);

            // Assert - the stop decided in Map is final; the source adapter does not ask again
            var call = Assert.Single(handlerCalls);
            Assert.IsType<InvalidOperationException>(call);
            Assert.True(sourceOperator.WasStoppedExplicitly);
        }

        #endregion

        #region Custom Error Handler Tests

        [Fact]
        public void SourceOperator_WithCustomErrorHandler_ReceivesCorrectContext()
        {
            // Arrange
            var capturedContexts = new List<StreamErrorContext>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                OnError = ctx =>
                {
                    capturedContexts.Add(ctx);
                    return ErrorHandlingDecision.Skip;
                }
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceCustomHandlerTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    if (x == 2) throw new InvalidOperationException("Test error");
                    return x * 10;
                })
                .Sink(x => { })
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);
            sourceOperator.EmitItem(2); // Should trigger error handler

            stream.Stop();

            // Assert
            Assert.Single(capturedContexts);
            Assert.Equal("SourceCustomHandlerTest", capturedContexts[0].StreamName);
            Assert.Equal(2, capturedContexts[0].Input);
            Assert.IsType<InvalidOperationException>(capturedContexts[0].Exception);
            Assert.Equal(1, capturedContexts[0].Attempt);
        }

        #endregion

        #region No Hanging/Looping Tests

        [Fact]
        public async Task SourceOperator_WithErrors_DoesNotHang()
        {
            // Arrange
            var sourceOperator = new TestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Skip
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceNoHangTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    if (x % 2 == 0) throw new InvalidOperationException("Even number error");
                    return x * 10;
                })
                .Sink(x => { })
                .Build();

            stream.Start();

            // Act - Emit many items including errors
            for (int i = 1; i <= 100; i++)
            {
                sourceOperator.EnqueueItem(i);
            }

            // Wait for processing with timeout - this should not hang
            var completed = await Task.Run(async () =>
            {
                await sourceOperator.WaitForProcessingAsync(TimeSpan.FromSeconds(5));
                return true;
            }).WaitAsync(TimeSpan.FromSeconds(10));

            stream.Stop();

            // Assert
            Assert.True(completed, "Processing should complete without hanging");
        }

        [Fact]
        public async Task SourceOperator_WithStopStrategy_DoesNotLoop()
        {
            // Arrange
            var emitCount = 0;
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Stop
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceNoLoopTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .Map(x =>
                {
                    Interlocked.Increment(ref emitCount);
                    throw new InvalidOperationException("Always fails");
#pragma warning disable CS0162
                    return x;
#pragma warning restore CS0162
                })
                .Sink(x => { })
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);

            // Small delay to ensure no looping
            await Task.Delay(100);

            // Assert - Should only process once, not loop
            Assert.Equal(1, emitCount);
            Assert.True(sourceOperator.WasStoppedExplicitly);
        }

        #endregion

        #region Integration with FlatMap Operator

        [Fact]
        public void SourceOperator_WithSkipStrategy_HandlesFlatMapErrors()
        {
            // Arrange
            var processedItems = new List<int>();
            var sourceOperator = new SynchronousTestSourceOperator<int>();
            var executionOptions = new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Skip
            };

            var stream = StreamBuilder<int>
                .CreateNewStream("SourceFlatMapTest")
                .WithErrorHandling(executionOptions)
                .Stream(sourceOperator)
                .FlatMap(x =>
                {
                    if (x == 2) throw new InvalidOperationException("FlatMap error");
                    return new[] { x, x * 10 };
                })
                .Sink(x => processedItems.Add(x))
                .Build();

            stream.Start();

            // Act
            sourceOperator.EmitItem(1);
            sourceOperator.EmitItem(2); // Should be skipped
            sourceOperator.EmitItem(3);

            stream.Stop();

            // Assert
            Assert.Equal(new[] { 1, 10, 3, 30 }, processedItems);
        }

        #endregion
    }
}
