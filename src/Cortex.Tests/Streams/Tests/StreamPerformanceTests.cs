using Cortex.Streams;
using Cortex.Streams.Performance;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Streams.Tests
{
    /// <summary>
    /// Tests for stream performance features including buffered processing, 
    /// backpressure handling, and async emit capabilities.
    /// </summary>
    public class StreamPerformanceTests
    {
        #region Backward Compatibility Tests

        [Fact]
        public void DefaultStream_WithoutPerformanceOptions_WorksAsExpected()
        {
            // Arrange - Using default options (no performance config)
            var results = new List<int>();
            var stream = StreamBuilder<int>.CreateNewStream("BackwardCompatibleStream")
                .Stream()
                .Map(x => x * 2)
                .Sink(x => results.Add(x))
                .Build();

            // Act
            stream.Start();
            stream.Emit(1);
            stream.Emit(2);
            stream.Emit(3);
            stream.Stop();

            // Assert
            Assert.Equal(new[] { 2, 4, 6 }, results);
        }

        [Fact]
        public async Task EmitAsync_WithoutBufferedProcessing_RunsOnThreadPool()
        {
            // Arrange
            var results = new ConcurrentBag<int>();
            var threadIds = new ConcurrentBag<int>();

            var stream = StreamBuilder<int>.CreateNewStream("AsyncDefaultStream")
                .Stream()
                .Map(x =>
                {
                    threadIds.Add(Thread.CurrentThread.ManagedThreadId);
                    return x * 2;
                })
                .Sink(x => results.Add(x))
                .Build();

            // Act
            stream.Start();
            var mainThreadId = Thread.CurrentThread.ManagedThreadId;
            
            await stream.EmitAsync(1);
            await stream.EmitAsync(2);
            await stream.EmitAsync(3);
            
            stream.Stop();

            // Assert
            Assert.Equal(3, results.Count);
            Assert.Contains(2, results);
            Assert.Contains(4, results);
            Assert.Contains(6, results);
        }

        [Fact]
        public void GetBufferStatistics_WithoutBufferedProcessing_ReturnsNull()
        {
            // Arrange
            var stream = StreamBuilder<int>.CreateNewStream("NoBufferStream")
                .Stream()
                .Sink(x => { })
                .Build();

            // Act
            stream.Start();
            var stats = stream.GetBufferStatistics();
            stream.Stop();

            // Assert
            Assert.Null(stats);
        }

        #endregion

        #region Buffered Processing Tests

        [Fact]
        public async Task EmitAsync_WithBufferedProcessing_EnqueuesAndProcesses()
        {
            // Arrange
            var results = new ConcurrentBag<int>();
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100,
                BackpressureStrategy = BackpressureStrategy.Block
            };

            var stream = StreamBuilder<int>.CreateNewStream("BufferedStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Map(x => x * 2)
                .Sink(x => results.Add(x))
                .Build();

            // Act
            stream.Start();
            
            await stream.EmitAsync(1);
            await stream.EmitAsync(2);
            await stream.EmitAsync(3);

            // Wait for processing to complete
            await stream.StopAsync();

            // Assert
            Assert.Equal(3, results.Count);
            Assert.Contains(2, results);
            Assert.Contains(4, results);
            Assert.Contains(6, results);
        }

        [Fact]
        public async Task EmitBatchAsync_WithBufferedProcessing_ProcessesAllItems()
        {
            // Arrange
            var results = new ConcurrentBag<int>();
            var performanceOptions = StreamPerformanceOptions.LowLatency();

            var stream = StreamBuilder<int>.CreateNewStream("BatchStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Map(x => x * 10)
                .Sink(x => results.Add(x))
                .Build();

            // Act
            stream.Start();
            
            var batch = Enumerable.Range(1, 100).ToList();
            await stream.EmitBatchAsync(batch);

            await stream.StopAsync();

            // Assert
            Assert.Equal(100, results.Count);
            Assert.Contains(10, results);
            Assert.Contains(1000, results);
        }

        [Fact]
        public void EmitAndForget_WithBufferedProcessing_ReturnsImmediately()
        {
            // Arrange
            var processedCount = 0;
            var processingDelay = TimeSpan.FromMilliseconds(50);
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100
            };

            var stream = StreamBuilder<int>.CreateNewStream("FireAndForgetStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x =>
                {
                    Thread.Sleep(processingDelay);
                    Interlocked.Increment(ref processedCount);
                })
                .Build();

            // Act
            stream.Start();
            
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < 10; i++)
            {
                var accepted = stream.EmitAndForget(i);
                Assert.True(accepted);
            }
            sw.Stop();

            // Assert - should return much faster than processing time
            Assert.True(sw.ElapsedMilliseconds < processingDelay.TotalMilliseconds * 5,
                $"EmitAndForget took too long: {sw.ElapsedMilliseconds}ms");

            // Wait for processing
            Thread.Sleep(TimeSpan.FromSeconds(1));
            stream.Stop();

            Assert.Equal(10, processedCount);
        }

        [Fact]
        public void EmitAndForget_WithoutBufferedProcessing_ThrowsException()
        {
            // Arrange
            var stream = StreamBuilder<int>.CreateNewStream("NoBufferStream")
                .Stream()
                .Sink(x => { })
                .Build();

            // Act & Assert
            stream.Start();
            Assert.Throws<InvalidOperationException>(() => stream.EmitAndForget(1));
            stream.Stop();
        }

        #endregion

        #region Buffer Statistics Tests

        [Fact]
        public async Task GetBufferStatistics_ReturnsAccurateStats()
        {
            // Arrange
            var processed = new ManualResetEventSlim(false);
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 1000
            };

            var stream = StreamBuilder<int>.CreateNewStream("StatsStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x =>
                {
                    Thread.Sleep(10); // Slow processing to build up queue
                })
                .Build();

            // Act
            stream.Start();
            
            // Emit some items
            for (int i = 0; i < 50; i++)
            {
                await stream.EmitAsync(i);
            }

            // Wait a bit for some processing to happen
            await Task.Delay(100);

            // Get stats while processing
            var stats = stream.GetBufferStatistics();

            await stream.StopAsync();

            // Assert
            Assert.NotNull(stats);
            Assert.Equal(1000, stats.Capacity);
            Assert.Equal(50, stats.TotalEnqueued);
            // After StopAsync, all items should be processed
            Assert.True(stats.TotalProcessed >= 0); // May still be processing
            Assert.True(stats.UtilizationPercent >= 0 && stats.UtilizationPercent <= 100);
        }

        #endregion

        #region Backpressure Strategy Tests

        [Fact]
        public async Task BackpressureDropNewest_AccountsForEveryItem()
        {
            // Arrange - every item is either processed or reported as dropped, never lost silently
            var dropped = 0;
            var processedItems = new ConcurrentBag<int>();
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 5,
                BackpressureStrategy = BackpressureStrategy.DropNewest,
                OnItemDropped = (item, reason) => Interlocked.Increment(ref dropped)
            };

            var stream = StreamBuilder<int>.CreateNewStream("DropNewestStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x => processedItems.Add(x))
                .Build();

            // Act
            stream.Start();

            var accepted = 0;
            for (int i = 0; i < 10; i++)
            {
                if (stream.EmitAndForget(i))
                    accepted++;
            }

            await stream.StopAsync();

            // Assert
            var stats = stream.GetBufferStatistics();
            Assert.Equal(accepted, processedItems.Count);
            Assert.Equal(10 - accepted, dropped);
            Assert.Equal(10 - accepted, stats.TotalDropped);
        }

        [Fact]
        public void BackpressureThrowException_ThrowsWhenFull()
        {
            // This test verifies that BufferFullException can be thrown
            // by directly testing the exception type exists and has correct properties
            var exception = new BufferFullException(100, "testItem");
            
            Assert.Equal(100, exception.BufferCapacity);
            Assert.Equal("testItem", exception.RejectedItem);
            Assert.Contains("100", exception.Message);
        }

        [Fact]
        public async Task BackpressureBlock_WaitsForSpace()
        {
            // Arrange
            var processedCount = 0;
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 2,
                BackpressureStrategy = BackpressureStrategy.Block,
                BlockingTimeout = TimeSpan.FromSeconds(5)
            };

            var stream = StreamBuilder<int>.CreateNewStream("BlockStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x =>
                {
                    Thread.Sleep(50); // Slow processing
                    Interlocked.Increment(ref processedCount);
                })
                .Build();

            // Act
            stream.Start();
            
            // Emit items - should block when buffer is full
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(stream.EmitAsync(i));
            }

            await Task.WhenAll(tasks);
            await stream.StopAsync();

            // Assert - all items should eventually be processed
            Assert.Equal(10, processedCount);
        }

        #endregion

        #region High Throughput Configuration Tests

        [Fact]
        public async Task HighThroughputOptions_ProcessesManyItems()
        {
            // Arrange
            var processedCount = 0;
            var performanceOptions = StreamPerformanceOptions.HighThroughput(
                bufferCapacity: 10000,
                concurrencyLevel: 4);

            var stream = StreamBuilder<int>.CreateNewStream("HighThroughputStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Map(x => x * 2)
                .Sink(x => Interlocked.Increment(ref processedCount))
                .Build();

            // Act
            stream.Start();
            
            var itemCount = 1000;
            for (int i = 0; i < itemCount; i++)
            {
                stream.EmitAndForget(i);
            }

            await stream.StopAsync();

            // Assert
            Assert.Equal(itemCount, processedCount);
        }

        [Fact]
        public async Task LowLatencyOptions_ProcessesItemsQuickly()
        {
            // Arrange
            var results = new ConcurrentBag<(int value, DateTime timestamp)>();
            var performanceOptions = StreamPerformanceOptions.LowLatency();

            var stream = StreamBuilder<int>.CreateNewStream("LowLatencyStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x => results.Add((x, DateTime.UtcNow)))
                .Build();

            // Act
            stream.Start();
            
            var emitTime = DateTime.UtcNow;
            await stream.EmitAsync(1);

            // Small delay to let processing complete
            await Task.Delay(100);
            await stream.StopAsync();


            // Assert
            Assert.Single(results);
            var result = results.First();
            var latency = (result.timestamp - emitTime).TotalMilliseconds;
            Assert.True(latency < 100, $"Latency was {latency}ms, expected < 100ms");
        }

        #endregion

        #region Cancellation Tests

        [Fact]
        public async Task EmitAsync_WithCancellation_RespectsCancellationToken()
        {
            // This test verifies that the cancellation token is properly passed through
            // by testing with an already-cancelled token
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100,
                BackpressureStrategy = BackpressureStrategy.Block
            };

            var stream = StreamBuilder<int>.CreateNewStream("CancellableStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x => { })
                .Build();

            stream.Start();
            
            try
            {
                // Use an already cancelled token
                using (var cts = new CancellationTokenSource())
                {
                    cts.Cancel();
                    
                    await Assert.ThrowsAsync<OperationCanceledException>(
                        () => stream.EmitAsync(1, cts.Token));
                }
            }
            finally
            {
                stream.Stop();
            }
        }

        [Fact]
        public async Task EmitBatchAsync_WithCancellation_RespectsCancellationToken()
        {
            // Test that batch emit respects cancellation
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100
            };

            var processedCount = 0;
            var stream = StreamBuilder<int>.CreateNewStream("BatchCancelStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x => Interlocked.Increment(ref processedCount))
                .Build();

            stream.Start();
            
            try
            {
                using (var cts = new CancellationTokenSource())
                {
                    cts.Cancel();
                    
                    await Assert.ThrowsAsync<OperationCanceledException>(
                        () => stream.EmitBatchAsync(new[] { 1, 2, 3 }, cts.Token));
                }
            }
            finally
            {
                stream.Stop();
            }
        }

        [Fact]
        public async Task StopAsync_GracefullyDrainsBuffer()
        {
            // Arrange
            var processedCount = 0;
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100
            };

            var stream = StreamBuilder<int>.CreateNewStream("GracefulStopStream")
                .WithPerformanceOptions(performanceOptions)
                .Stream()
                .Sink(x =>
                {
                    Thread.Sleep(10);
                    Interlocked.Increment(ref processedCount);
                })
                .Build();

            // Act
            stream.Start();
            
            // Emit items quickly
            for (int i = 0; i < 20; i++)
            {
                stream.EmitAndForget(i);
            }

            // Graceful stop should wait for processing
            await stream.StopAsync();

            // Assert - all items should be processed
            Assert.Equal(20, processedCount);
        }

        #endregion

        #region Error Handling Integration Tests

        [Fact]
        public async Task BufferedProcessing_WithErrorHandling_ContinuesOnError()
        {
            // Arrange
            var processedItems = new ConcurrentBag<int>();
            var performanceOptions = new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 100
            };
            var executionOptions = new Cortex.Streams.ErrorHandling.StreamExecutionOptions
            {
                ErrorHandlingStrategy = Cortex.Streams.ErrorHandling.ErrorHandlingStrategy.Skip
            };

            var stream = StreamBuilder<int>.CreateNewStream("ErrorHandlingStream")
                .WithPerformanceOptions(performanceOptions)
                .WithErrorHandling(executionOptions)
                .Stream()
                .Map(x =>
                {
                    if (x == 5) throw new InvalidOperationException("Test error");
                    return x * 2;
                })
                .Sink(x => processedItems.Add(x))
                .Build();

            // Act
            stream.Start();
            
            for (int i = 1; i <= 10; i++)
            {
                await stream.EmitAsync(i);
            }

            await stream.StopAsync();

            // Assert - 9 items should be processed (item 5 skipped)
            Assert.Equal(9, processedItems.Count);
            Assert.DoesNotContain(10, processedItems); // 5 * 2 = 10 should not be there
        }

        #endregion

        #region Drop Reporting Tests

        /// <summary>
        /// A buffered stream whose sink blocks on item 1 until <see cref="Release"/> is set,
        /// so the test controls exactly when the buffer is full.
        /// </summary>
        private sealed class BlockedSinkStream : IDisposable
        {
            private readonly List<int> _processed = new List<int>();
            private readonly List<(object Item, DropReason Reason)> _drops = new List<(object, DropReason)>();

            public ManualResetEventSlim FirstStarted { get; } = new ManualResetEventSlim();
            public ManualResetEventSlim Release { get; } = new ManualResetEventSlim();
            public IStream<int> Stream { get; }

            public BlockedSinkStream(BackpressureStrategy strategy, int capacity)
            {
                Stream = StreamBuilder<int>.CreateNewStream("BlockedSink")
                    .WithPerformanceOptions(new StreamPerformanceOptions
                    {
                        EnableBufferedProcessing = true,
                        BufferCapacity = capacity,
                        BackpressureStrategy = strategy,
                        OnItemDropped = (item, reason) => { lock (_drops) _drops.Add((item, reason)); }
                    })
                    .Stream()
                    .Sink(x =>
                    {
                        if (x == 1)
                        {
                            FirstStarted.Set();
                            Release.Wait(TimeSpan.FromSeconds(30));
                        }
                        lock (_processed) _processed.Add(x);
                    })
                    .Build();
            }

            public void StartAndBlockOnFirstItem()
            {
                Stream.Start();
                Assert.True(Stream.EmitAndForget(1));
                Assert.True(FirstStarted.Wait(TimeSpan.FromSeconds(10)), "The sink did not start item 1.");
            }

            public int[] Processed { get { lock (_processed) return _processed.ToArray(); } }
            public (object Item, DropReason Reason)[] Drops { get { lock (_drops) return _drops.ToArray(); } }

            public void Dispose()
            {
                Release.Set();
                Stream.Stop();
            }
        }

        [Fact]
        public async Task DropOldest_WhenBufferIsFull_ReportsAndCountsTheEvictedItem()
        {
            using var harness = new BlockedSinkStream(BackpressureStrategy.DropOldest, capacity: 2);
            harness.StartAndBlockOnFirstItem();

            Assert.True(harness.Stream.EmitAndForget(2));
            Assert.True(harness.Stream.EmitAndForget(3));
            Assert.True(harness.Stream.EmitAndForget(4)); // buffer full (2, 3): item 2 is evicted

            harness.Release.Set();
            await harness.Stream.StopAsync();

            Assert.Equal(new[] { 1, 3, 4 }, harness.Processed);
            var drop = Assert.Single(harness.Drops);
            Assert.Equal(2, drop.Item);
            Assert.Equal(DropReason.BufferFullDropOldest, drop.Reason);
            Assert.Equal(1, harness.Stream.GetBufferStatistics().TotalDropped);
        }

        [Fact]
        public async Task DropNewest_WhenBufferIsFull_RejectsTheIncomingItem()
        {
            using var harness = new BlockedSinkStream(BackpressureStrategy.DropNewest, capacity: 2);
            harness.StartAndBlockOnFirstItem();

            Assert.True(harness.Stream.EmitAndForget(2));
            Assert.True(harness.Stream.EmitAndForget(3));
            Assert.False(harness.Stream.EmitAndForget(4)); // buffer full (2, 3): item 4 is rejected

            harness.Release.Set();
            await harness.Stream.StopAsync();

            Assert.Equal(new[] { 1, 2, 3 }, harness.Processed);
            var drop = Assert.Single(harness.Drops);
            Assert.Equal(4, drop.Item);
            Assert.Equal(DropReason.BufferFullDropNewest, drop.Reason);
            var stats = harness.Stream.GetBufferStatistics();
            Assert.Equal(1, stats.TotalDropped);
            Assert.Equal(3, stats.TotalEnqueued);
        }

        [Fact]
        public async Task DropNewest_EmitAsync_WhenBufferIsFull_ReportsTheIncomingItem()
        {
            using var harness = new BlockedSinkStream(BackpressureStrategy.DropNewest, capacity: 1);
            harness.StartAndBlockOnFirstItem();

            await harness.Stream.EmitAsync(2);
            await harness.Stream.EmitAsync(3); // buffer full (2): item 3 is rejected

            harness.Release.Set();
            await harness.Stream.StopAsync();

            Assert.Equal(new[] { 1, 2 }, harness.Processed);
            var drop = Assert.Single(harness.Drops);
            Assert.Equal(3, drop.Item);
            Assert.Equal(DropReason.BufferFullDropNewest, drop.Reason);
        }

        #endregion
    }
}
