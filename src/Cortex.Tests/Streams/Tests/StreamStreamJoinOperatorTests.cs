using Cortex.States;
using Cortex.Streams;
using Cortex.Streams.ErrorHandling;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Telemetry;
using Moq;

namespace Cortex.Tests.Streams.Tests
{
    public class StreamStreamJoinOperatorTests : IDisposable
    {
        private readonly List<IDisposable> _disposables = new();

        public void Dispose()
        {
            foreach (var d in _disposables)
            {
                d.Dispose();
            }
        }

        #region Test Models

        public record Order(string OrderId, int CustomerId, decimal Amount, DateTime Timestamp);
        public record Shipment(string ShipmentId, string OrderId, DateTime ShippedAt, DateTime Timestamp);
        public record OrderShipment(Order Order, Shipment? Shipment);

        #endregion

        #region Inner Join Tests

        [Fact]
        public void InnerJoin_WithMatchingKeys_ShouldEmitJoinedResult()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order, shipment),
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act
            var order = new Order("ORD-001", 100, 50.00m, baseTime);
            var shipment = new Shipment("SHP-001", "ORD-001", baseTime.AddMinutes(1), baseTime.AddMinutes(1));

            joinOperator.ProcessLeft(order);
            joinOperator.ProcessRight(shipment);

            // Assert
            Assert.Single(results);
            Assert.Equal("ORD-001", results[0].Order.OrderId);
            Assert.Equal("SHP-001", results[0].Shipment?.ShipmentId);
        }

        [Fact]
        public void InnerJoin_WithNoMatchingKeys_ShouldNotEmit()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order, shipment),
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act
            var order = new Order("ORD-001", 100, 50.00m, baseTime);
            var shipment = new Shipment("SHP-001", "ORD-999", baseTime.AddMinutes(1), baseTime.AddMinutes(1));

            joinOperator.ProcessLeft(order);
            joinOperator.ProcessRight(shipment);

            // Assert - inner join should not emit when keys don't match
            Assert.Empty(results);
        }

        [Fact]
        public void InnerJoin_RightArrivesBeforeLeft_ShouldStillMatch()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order, shipment),
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act - shipment arrives BEFORE order
            var shipment = new Shipment("SHP-001", "ORD-001", baseTime, baseTime);
            var order = new Order("ORD-001", 100, 50.00m, baseTime.AddSeconds(30));

            joinOperator.ProcessRight(shipment);
            joinOperator.ProcessLeft(order);

            // Assert
            Assert.Single(results);
            Assert.Equal("ORD-001", results[0].Order.OrderId);
        }

        [Fact]
        public void InnerJoin_MultipleMatchesPerKey_ShouldEmitAll()
        {
            // Arrange
            var results = new List<string>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<int, int, int, string>(
                left => left,
                right => right,
                _ => baseTime,
                _ => baseTime,
                (left, right) => $"L{left}-R{right}",
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<string>(x => results.Add(x)));

            // Act - send multiple elements with same key
            joinOperator.ProcessLeft(1);
            joinOperator.ProcessLeft(1);  // Second left with same key
            joinOperator.ProcessRight(1); // Should match both lefts

            // Assert
            Assert.Equal(2, results.Count);
        }

        #endregion

        #region Left Join Tests

        [Fact]
        public void LeftJoin_WithNoMatch_ShouldEmitOnWindowExpiration()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow.AddMinutes(-10); // Past time so it expires quickly

            var config = new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMilliseconds(50),
                JoinType = StreamJoinType.Left,
                CleanupInterval = TimeSpan.FromMilliseconds(25),
                GracePeriod = TimeSpan.Zero
            };

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order, shipment!),
                config);

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act
            var order = new Order("ORD-001", 100, 50.00m, baseTime);
            joinOperator.ProcessLeft(order);

            // Wait for window to expire and cleanup to run
            Thread.Sleep(150);
            joinOperator.ForceCleanup();

            // Assert - left join should emit unmatched left element with null right
            Assert.Single(results);
            Assert.Equal("ORD-001", results[0].Order.OrderId);
            Assert.Null(results[0].Shipment);
        }

        [Fact]
        public void LeftJoin_WithMatch_ShouldEmitImmediately()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow;

            var config = StreamJoinConfiguration.LeftJoin(TimeSpan.FromMinutes(5));

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order, shipment),
                config);

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act
            joinOperator.ProcessLeft(new Order("ORD-001", 100, 50.00m, baseTime));
            joinOperator.ProcessRight(new Shipment("SHP-001", "ORD-001", baseTime, baseTime));

            // Assert
            Assert.Single(results);
            Assert.NotNull(results[0].Shipment);
        }

        #endregion

        #region Outer Join Tests

        [Fact]
        public void OuterJoin_BothSidesUnmatched_ShouldEmitBothOnExpiration()
        {
            // Arrange
            var results = new List<OrderShipment>();
            var baseTime = DateTime.UtcNow.AddMinutes(-10);

            var config = new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMilliseconds(50),
                JoinType = StreamJoinType.Outer,
                CleanupInterval = TimeSpan.FromMilliseconds(25),
                GracePeriod = TimeSpan.Zero
            };

            var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
                order => order.OrderId,
                shipment => shipment.OrderId,
                order => order.Timestamp,
                shipment => shipment.Timestamp,
                (order, shipment) => new OrderShipment(order!, shipment!),
                config);

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<OrderShipment>(x => results.Add(x)));

            // Act - no matching keys
            joinOperator.ProcessLeft(new Order("ORD-001", 100, 50.00m, baseTime));
            joinOperator.ProcessRight(new Shipment("SHP-999", "ORD-999", baseTime, baseTime));

            // Wait for expiration
            Thread.Sleep(150);
            joinOperator.ForceCleanup();

            // Assert - outer join emits both unmatched sides
            Assert.Equal(2, results.Count);
        }

        #endregion

        #region Window Expiration Tests

        [Fact]
        public void WindowExpiration_ShouldRemoveOldElements()
        {
            // Arrange
            var baseTime = DateTime.UtcNow.AddMinutes(-10);

            var config = new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMilliseconds(50),
                JoinType = StreamJoinType.Inner,
                CleanupInterval = TimeSpan.FromMilliseconds(25)
            };

            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left,
                right => right,
                _ => baseTime,
                _ => baseTime,
                (left, right) => left + right,
                config);

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<int>(_ => { }));

            // Act
            joinOperator.ProcessLeft(1);
            joinOperator.ProcessRight(2);

            Assert.True(joinOperator.GetLeftBufferCount() > 0 || joinOperator.GetRightBufferCount() > 0);

            // Wait for cleanup
            Thread.Sleep(150);
            joinOperator.ForceCleanup();

            // Assert - buffers should be cleared
            Assert.Equal(0, joinOperator.GetLeftBufferCount());
            Assert.Equal(0, joinOperator.GetRightBufferCount());
        }

        #endregion

        #region Buffer Management Tests

        [Fact]
        public void MaxBufferSize_ShouldEvictOldestElements()
        {
            // Arrange
            var config = new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromHours(1),
                MaxBufferSizePerKey = 3
            };

            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => 1, // All same key
                right => 1,
                _ => DateTime.UtcNow,
                _ => DateTime.UtcNow,
                (left, right) => left + right,
                config);

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<int>(_ => { }));

            // Act - add more than max buffer size
            for (int i = 0; i < 10; i++)
            {
                joinOperator.ProcessLeft(i);
            }

            // Assert - should only have MaxBufferSizePerKey elements
            Assert.Equal(3, joinOperator.GetLeftBufferCount());
        }

        [Fact]
        public void GetBufferCounts_ShouldReturnCorrectCounts()
        {
            // Arrange
            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left,
                right => right,
                _ => DateTime.UtcNow,
                _ => DateTime.UtcNow,
                (left, right) => left + right,
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromHours(1)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<int>(_ => { }));

            // Act
            joinOperator.ProcessLeft(1);
            joinOperator.ProcessLeft(2);
            joinOperator.ProcessLeft(3);
            joinOperator.ProcessRight(10);
            joinOperator.ProcessRight(20);

            // Assert
            Assert.Equal(3, joinOperator.GetLeftBufferCount());
            Assert.Equal(2, joinOperator.GetRightBufferCount());
        }

        #endregion

        #region Constructor Validation Tests

        [Fact]
        public void Constructor_WithNullLeftKeySelector_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StreamStreamJoinOperator<int, int, int, int>(
                    null!,
                    right => right,
                    _ => DateTime.UtcNow,
                    _ => DateTime.UtcNow,
                    (left, right) => left + right));
        }

        [Fact]
        public void Constructor_WithNullRightKeySelector_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StreamStreamJoinOperator<int, int, int, int>(
                    left => left,
                    null!,
                    _ => DateTime.UtcNow,
                    _ => DateTime.UtcNow,
                    (left, right) => left + right));
        }

        [Fact]
        public void Constructor_WithNullJoinFunction_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new StreamStreamJoinOperator<int, int, int, int>(
                    left => left,
                    right => right,
                    _ => DateTime.UtcNow,
                    _ => DateTime.UtcNow,
                    null!));
        }

        #endregion

        #region Concurrent Access Tests

        [Fact]
        public async Task ConcurrentAccess_ShouldBeThreadSafe()
        {
            // Arrange
            var results = new System.Collections.Concurrent.ConcurrentBag<int>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left % 10,  // Group into 10 keys
                right => right % 10,
                _ => baseTime,
                _ => baseTime,
                (left, right) => left + right,
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromHours(1)));

            _disposables.Add(joinOperator);
            joinOperator.SetNext(new SinkOperator<int>(x => results.Add(x)));

            // Act - concurrent processing
            var leftTasks = Enumerable.Range(0, 100)
                .Select(i => Task.Run(() => joinOperator.ProcessLeft(i)));

            var rightTasks = Enumerable.Range(0, 100)
                .Select(i => Task.Run(() => joinOperator.ProcessRight(i)));

            await Task.WhenAll(leftTasks.Concat(rightTasks));

            // Assert - should have processed without exceptions
            Assert.True(results.Count > 0, "Should have some matched results");
        }

        #endregion

        #region Integration with StreamBuilder Tests

        [Fact]
        public void StreamBuilder_JoinStream_ShouldWorkInPipeline()
        {
            // Arrange
            var results = new List<string>();
            var baseTime = DateTime.UtcNow;

            var joinOperator = new StreamStreamJoinOperator<int, string, int, string>(
                left => left,
                right => int.Parse(right.Split(':')[0]),
                _ => baseTime,
                _ => baseTime,
                (left, right) => $"Joined:{left}-{right}",
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5)));

            _disposables.Add(joinOperator);

            var stream = StreamBuilder<int>.CreateNewStream("JoinStreamTest")
                .Stream()
                .JoinStream(joinOperator)
                .Sink(x => results.Add(x))
                .Build();

            // Act
            stream.Start();
            stream.Emit(1);
            stream.Emit(2);

            // Feed right side directly to join operator
            joinOperator.ProcessRight("1:DataA");
            joinOperator.ProcessRight("2:DataB");

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Contains("Joined:1-1:DataA", results);
            Assert.Contains("Joined:2-2:DataB", results);
        }

        #endregion

        #region Telemetry Tests

        [Fact]
        public void Telemetry_ShouldTrackProcessedAndMatchedCounters()
        {
            // Arrange
            var (mockProvider, state) = CreateMockTelemetryProvider();

            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left,
                right => right,
                _ => DateTime.UtcNow,
                _ => DateTime.UtcNow,
                (left, right) => left + right,
                StreamJoinConfiguration.InnerJoin(TimeSpan.FromHours(1)));

            _disposables.Add(joinOperator);
            joinOperator.SetTelemetryProvider(mockProvider.Object);
            joinOperator.SetNext(new SinkOperator<int>(_ => { }));

            // Act
            joinOperator.ProcessLeft(1);
            joinOperator.ProcessLeft(2);
            joinOperator.ProcessRight(1);  // Matches key 1
            joinOperator.ProcessRight(3);  // No match

            // Assert
            var leftProcessed = state.GetCounterValue("stream_stream_join_Int32_Int32_left_processed");
            var rightProcessed = state.GetCounterValue("stream_stream_join_Int32_Int32_right_processed");
            var matched = state.GetCounterValue("stream_stream_join_Int32_Int32_matched");

            Assert.Equal(2, leftProcessed);
            Assert.Equal(2, rightProcessed);
            Assert.Equal(1, matched);  // Only key 1 matched
        }

        #region Mock Telemetry Infrastructure

        private static (Mock<ITelemetryProvider> provider, MockTelemetryState state) CreateMockTelemetryProvider()
        {
            var state = new MockTelemetryState();
            var mockProvider = new Mock<ITelemetryProvider>();
            var mockMetricsProvider = new Mock<IMetricsProvider>();
            var mockTracingProvider = new Mock<ITracingProvider>();

            mockMetricsProvider.Setup(m => m.CreateCounter(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string name, string desc) => new MockCounter(name, state));

            mockMetricsProvider.Setup(m => m.CreateHistogram(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string name, string desc) => new MockHistogram(name, state));

            mockTracingProvider.Setup(t => t.GetTracer(It.IsAny<string>()))
                .Returns((string name) => new MockTracer(name, state));

            mockProvider.Setup(p => p.GetMetricsProvider()).Returns(mockMetricsProvider.Object);
            mockProvider.Setup(p => p.GetTracingProvider()).Returns(mockTracingProvider.Object);

            return (mockProvider, state);
        }

        private class MockTelemetryState
        {
            private readonly object _lock = new();
            public Dictionary<string, double> CounterValues { get; } = new();

            public void IncrementCounter(string name, double value)
            {
                lock (_lock)
                {
                    if (!CounterValues.ContainsKey(name))
                        CounterValues[name] = 0;
                    CounterValues[name] += value;
                }
            }

            public double GetCounterValue(string name)
            {
                lock (_lock)
                {
                    return CounterValues.TryGetValue(name, out var value) ? value : 0;
                }
            }
        }

        private class MockCounter : ICounter
        {
            private readonly string _name;
            private readonly MockTelemetryState _state;

            public MockCounter(string name, MockTelemetryState state)
            {
                _name = name;
                _state = state;
                _state.IncrementCounter(name, 0);
            }

            public void Increment(double value = 1) => _state.IncrementCounter(_name, value);
        }

        private class MockHistogram : IHistogram
        {
            public MockHistogram(string name, MockTelemetryState state) { }
            public void Record(double value) { }
        }

        private class MockTracer : ITracer
        {
            public MockTracer(string name, MockTelemetryState state) { }
            public ISpan StartSpan(string name) => new MockSpan();
        }

        private class MockSpan : ISpan
        {
            public void SetAttribute(string key, string value) { }
            public void AddEvent(string name, IDictionary<string, object>? attributes = null) { }
            public void Dispose() { }
        }

        #endregion

        #endregion

        #region State Store Tests

        [Fact]
        public void GetStateStores_WithNoStores_ShouldReturnEmpty()
        {
            // Arrange
            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left,
                right => right,
                _ => DateTime.UtcNow,
                _ => DateTime.UtcNow,
                (left, right) => left + right);

            _disposables.Add(joinOperator);

            // Act
            var stores = joinOperator.GetStateStores().ToList();

            // Assert
            Assert.Empty(stores);
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_ShouldStopCleanupTimer()
        {
            // Arrange
            var joinOperator = new StreamStreamJoinOperator<int, int, int, int>(
                left => left,
                right => right,
                _ => DateTime.UtcNow,
                _ => DateTime.UtcNow,
                (left, right) => left + right);

            // Act & Assert - should not throw
            joinOperator.Dispose();
            joinOperator.Dispose(); // Double dispose should be safe
        }

        #endregion

        #region Expiry Error Handling Tests

        private static readonly DateTime LongAgo = DateTime.UtcNow.AddMinutes(-10);

        private static StreamJoinConfiguration ExpiringLeftJoin(TimeSpan cleanupInterval) => new StreamJoinConfiguration
        {
            WindowSize = TimeSpan.FromMilliseconds(50),
            JoinType = StreamJoinType.Left,
            CleanupInterval = cleanupInterval,
            GracePeriod = TimeSpan.Zero
        };

        private StreamStreamJoinOperator<int, int, int, string> CreateExpiringJoin(
            Func<int, int, string> joinFunction, TimeSpan cleanupInterval)
        {
            var joinOperator = new StreamStreamJoinOperator<int, int, int, string>(
                left => left,
                right => right,
                _ => LongAgo,
                _ => LongAgo,
                joinFunction,
                ExpiringLeftJoin(cleanupInterval));
            _disposables.Add(joinOperator);
            return joinOperator;
        }

        [Fact]
        public void ForceCleanup_WithSkipStrategy_SkipsAFailingUnmatchedResultAndEmitsTheRest()
        {
            var results = new List<string>();
            var joinOperator = CreateExpiringJoin(
                (left, right) => left == 1 ? throw new InvalidOperationException("bad") : $"L{left}",
                TimeSpan.FromHours(1));
            joinOperator.SetErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip });
            joinOperator.SetNext(new SinkOperator<string>(x => results.Add(x)));

            joinOperator.ProcessLeft(1);
            joinOperator.ProcessLeft(2);
            joinOperator.ForceCleanup();

            Assert.Equal(new[] { "L2" }, results);
            Assert.Equal(0, joinOperator.GetLeftBufferCount());
        }

        [Fact]
        public void ForceCleanup_RightJoin_WithSkipStrategy_SkipsAFailingDownstreamResult()
        {
            var results = new List<string>();
            var joinOperator = new StreamStreamJoinOperator<int, int, int, string>(
                left => left,
                right => right,
                _ => LongAgo,
                _ => LongAgo,
                (left, right) => $"R{right}",
                new StreamJoinConfiguration
                {
                    WindowSize = TimeSpan.FromMilliseconds(50),
                    JoinType = StreamJoinType.Right,
                    CleanupInterval = TimeSpan.FromHours(1),
                    GracePeriod = TimeSpan.Zero
                });
            _disposables.Add(joinOperator);
            joinOperator.SetErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip });
            joinOperator.SetNext(new SinkOperator<string>(x =>
            {
                if (x == "R1") throw new InvalidOperationException("sink failed");
                results.Add(x);
            }));

            joinOperator.ProcessRight(1);
            joinOperator.ProcessRight(2);
            joinOperator.ForceCleanup();

            Assert.Equal(new[] { "R2" }, results);
            Assert.Equal(0, joinOperator.GetRightBufferCount());
        }

        [Fact]
        public void ForceCleanup_WithoutErrorHandling_ThrowsToTheCaller()
        {
            var joinOperator = CreateExpiringJoin(
                (left, right) => throw new InvalidOperationException("bad"),
                TimeSpan.FromHours(1));
            joinOperator.SetNext(new SinkOperator<string>(x => { }));

            joinOperator.ProcessLeft(1);

            Assert.Throws<InvalidOperationException>(() => joinOperator.ForceCleanup());
        }

        [Fact]
        public void ForceCleanup_EmitsUnmatchedResultsOutsideTheBufferLock()
        {
            var lockWasFree = false;
            StreamStreamJoinOperator<int, int, int, string> joinOperator = null!;
            joinOperator = CreateExpiringJoin((left, right) => $"L{left}", TimeSpan.FromHours(1));
            joinOperator.SetNext(new SinkOperator<string>(x =>
            {
                // Another thread needs the buffer lock; it can only get it if cleanup released it
                lockWasFree = Task.Run(() => joinOperator.GetLeftBufferCount()).Wait(TimeSpan.FromSeconds(2));
            }));

            joinOperator.ProcessLeft(1);
            joinOperator.ForceCleanup();

            Assert.True(lockWasFree);
        }

        [Fact]
        public void CleanupTimer_UnhandledJoinException_DoesNotStopLaterResults()
        {
            var results = new List<string>();
            var joinOperator = CreateExpiringJoin(
                (left, right) => left == 1 ? throw new InvalidOperationException("bad") : $"L{left}",
                TimeSpan.FromMilliseconds(50));
            joinOperator.SetNext(new SinkOperator<string>(x => { lock (results) results.Add(x); }));

            joinOperator.ProcessLeft(1);
            joinOperator.ProcessLeft(2);

            // The exception for item 1 stays on the timer: the process keeps running and item 2 is emitted
            Assert.True(SpinWait.SpinUntil(() => { lock (results) return results.Contains("L2"); }, TimeSpan.FromSeconds(5)));
            lock (results) Assert.Equal(new[] { "L2" }, results);
        }

        [Fact]
        public void CleanupTimer_WithStopStrategy_StopsTheStream()
        {
            var joinOperator = CreateExpiringJoin(
                (left, right) => throw new InvalidOperationException("bad"),
                TimeSpan.FromMilliseconds(50));

            var stream = StreamBuilder<int>.CreateNewStream("JoinExpiryStop")
                .WithErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Stop })
                .Stream()
                .JoinStream(joinOperator)
                .Sink(x => { })
                .Build();

            stream.Start();
            stream.Emit(1);

            Assert.True(SpinWait.SpinUntil(() => stream.GetStatus() == StreamStatuses.NOT_RUNNING, TimeSpan.FromSeconds(5)));
        }

        #endregion
    }
}
