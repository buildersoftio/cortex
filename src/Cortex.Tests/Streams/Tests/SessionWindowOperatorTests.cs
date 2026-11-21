using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Windows;
using Moq;

namespace Cortex.Streams.Tests
{
    public class SessionWindowOperatorTests
    {
        [Fact]
        public void SessionWindowOperator_GroupsItemsIntoSession()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            try
            {
                // Act - emit items within the same session
                windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
                windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(1) });

                // Wait for the session to expire (bounded)
                Assert.True(WaitForCount(emittedResults, 1, TimeSpan.FromSeconds(10)), "The session was not emitted.");

                // Assert
                var results = Snapshot(emittedResults);
                Assert.Single(results);
                Assert.Equal("A", results[0].Key);
                Assert.Equal(2, results[0].Items.Count);
                Assert.Equal(3, results[0].Items.Sum(x => x.Value));
            }
            finally
            {
                windowOperator.Dispose();
            }
        }

        [Fact]
        public void SessionWindowOperator_SeparatesSessionsByKey()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act - emit items for different keys
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "B", Value = 10, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(500) });
            windowOperator.Process(new InputData { Key = "B", Value = 20, EventTime = now.AddMilliseconds(500) });

            // Wait for sessions to expire
            Thread.Sleep(3000);

            // Assert
            Assert.Equal(2, emittedResults.Count);

            var keyAResult = emittedResults.FirstOrDefault(r => r.Key == "A");
            var keyBResult = emittedResults.FirstOrDefault(r => r.Key == "B");

            Assert.NotNull(keyAResult);
            Assert.NotNull(keyBResult);
            Assert.Equal(2, keyAResult.Items.Count);
            Assert.Equal(3, keyAResult.Items.Sum(x => x.Value));
            Assert.Equal(2, keyBResult.Items.Count);
            Assert.Equal(30, keyBResult.Items.Sum(x => x.Value));

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void SessionWindowOperator_CreatesNewSessionAfterInactivityGap()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(1);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            try
            {
                // Act - first session
                windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });

                // Wait for the first session to expire (bounded)
                Assert.True(WaitForCount(emittedResults, 1, TimeSpan.FromSeconds(10)), "The first session was not emitted.");

                // Second session, after the inactivity gap
                windowOperator.Process(new InputData { Key = "A", Value = 5, EventTime = now.AddSeconds(3) });

                // Wait for the second session to expire (bounded)
                Assert.True(WaitForCount(emittedResults, 2, TimeSpan.FromSeconds(10)), "The second session was not emitted.");

                // Assert
                var results = Snapshot(emittedResults);
                Assert.Equal(2, results.Count);
                Assert.Equal(1, results[0].Items.Sum(x => x.Value));
                Assert.Equal(5, results[1].Items.Sum(x => x.Value));
            }
            finally
            {
                windowOperator.Dispose();
            }
        }

        private static bool WaitForCount<T>(List<T> items, int count, TimeSpan timeout)
        {
            return SpinWait.SpinUntil(() => { lock (items) { return items.Count >= count; } }, timeout);
        }

        private static List<T> Snapshot<T>(List<T> items)
        {
            lock (items)
            {
                return items.ToList();
            }
        }

        [Fact]
        public void SessionWindowOperator_ExtendsSessionWithActivityWithinGap()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act - emit items keeping the session alive with real-time delay
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            Thread.Sleep(500);
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = DateTime.UtcNow });
            Thread.Sleep(500);
            windowOperator.Process(new InputData { Key = "A", Value = 3, EventTime = DateTime.UtcNow });

            // Wait for session to expire
            Thread.Sleep(3000);

            // Assert - all items should be in a single session
            Assert.Single(emittedResults);
            Assert.Equal(3, emittedResults[0].Items.Count);
            Assert.Equal(6, emittedResults[0].Items.Sum(x => x.Value));

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void SessionWindowOperator_ThrowsOnNullKeySelector()
        {
            // Arrange & Act & Assert
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");

            Assert.Throws<ArgumentNullException>(() =>
                new SessionWindowOperator<InputData, string>(
                    keySelector: null,
                    timestampSelector: x => x.EventTime,
                    inactivityGap: TimeSpan.FromSeconds(1),
                    stateStore: stateStore));
        }

        [Fact]
        public void SessionWindowOperator_ThrowsOnNullTimestampSelector()
        {
            // Arrange & Act & Assert
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");

            Assert.Throws<ArgumentNullException>(() =>
                new SessionWindowOperator<InputData, string>(
                    keySelector: x => x.Key,
                    timestampSelector: null,
                    inactivityGap: TimeSpan.FromSeconds(1),
                    stateStore: stateStore));
        }

        [Fact]
        public void SessionWindowOperator_ThrowsOnNullStateStore()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new SessionWindowOperator<InputData, string>(
                    keySelector: x => x.Key,
                    timestampSelector: x => x.EventTime,
                    inactivityGap: TimeSpan.FromSeconds(1),
                    stateStore: null));
        }

        [Fact]
        public void SessionWindowOperator_ThreadSafety_NoExceptionsThrown()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();
            var lockObj = new object();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (lockObj)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act - emit items from multiple threads
            var tasks = new List<Task>();
            for (int i = 0; i < 50; i++)
            {
                int value = i;
                tasks.Add(Task.Run(() =>
                {
                    windowOperator.Process(new InputData { Key = "A", Value = value, EventTime = now });
                }));
            }

            Task.WaitAll(tasks.ToArray());

            // Wait for session to expire
            Thread.Sleep(3000);

            // Assert - no exceptions thrown and session was emitted
            Assert.Single(emittedResults);
            int totalSum = emittedResults[0].Items.Sum(x => x.Value);
            int expectedSum = Enumerable.Range(0, 50).Sum();
            Assert.Equal(expectedSum, totalSum);

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void SessionWindowOperator_IntegrationWithStreamBuilder()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var emittedResults = new List<WindowResult<string, InputData>>();

            var stream = StreamBuilder<InputData>
                .CreateNewStream("Test Session Window Stream")
                .Stream()
                .SessionWindow<string>(
                    keySelector: x => x.Key,
                    timestampSelector: x => x.EventTime,
                    inactivityGap: inactivityGap)
                .Sink(result =>
                {
                    lock (emittedResults)
                    {
                        emittedResults.Add(result);
                    }
                })
                .Build();

            stream.Start();

            var now = DateTime.UtcNow;

            // Act
            stream.Emit(new InputData { Key = "A", Value = 1, EventTime = now });
            stream.Emit(new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(1) });

            // Wait for session to expire
            Thread.Sleep(3500);

            // Assert
            Assert.Single(emittedResults);
            Assert.Equal(2, emittedResults[0].Items.Count);
            Assert.Equal(3, emittedResults[0].Items.Sum(x => x.Value));

            stream.Stop();
        }

        [Fact]
        public void SessionWindowOperator_SessionBoundariesAreCorrect()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var sessionStart = DateTime.UtcNow;

            // Act
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = sessionStart });
            Thread.Sleep(500);
            var sessionEnd = DateTime.UtcNow;
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = sessionEnd });

            // Wait for session to expire
            Thread.Sleep(3000);

            // Assert - verify session boundaries
            Assert.Single(emittedResults);
            Assert.True(emittedResults[0].WindowStart <= sessionStart.AddMilliseconds(100)); // Allow small tolerance
            // Window end should be last activity time + inactivity gap
            Assert.True(emittedResults[0].WindowEnd >= sessionEnd);

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void SessionWindowOperator_StatePersistence_StateRestoredCorrectly()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();
            var lockObj = new object();

            Action<WindowResult<string, InputData>> sinkAction = result =>
            {
                lock (lockObj)
                {
                    emittedResults.Add(result);
                }
            };

            // First operator instance
            var windowOperator1 = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator1 = new SinkOperator<WindowResult<string, InputData>>(sinkAction);
            windowOperator1.SetNext(sinkOperator1);

            var now = DateTime.UtcNow;

            // Act - add data to first instance
            windowOperator1.Process(new InputData { Key = "A", Value = 1, EventTime = now });

            // Stop the timer but keep the state (simulate restart)
            windowOperator1.Dispose();

            // Wait a bit but less than inactivity gap
            Thread.Sleep(500);

            // Create second operator instance with same state store
            var windowOperator2 = new SessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore);

            var sinkOperator2 = new SinkOperator<WindowResult<string, InputData>>(sinkAction);
            windowOperator2.SetNext(sinkOperator2);

            // Add more data with current time
            windowOperator2.Process(new InputData { Key = "A", Value = 2, EventTime = DateTime.UtcNow });

            // Wait for session to expire
            Thread.Sleep(3000);

            // Assert - both items should be in the same session
            Assert.Single(emittedResults);
            Assert.Equal(3, emittedResults[0].Items.Sum(x => x.Value));

            // Cleanup
            windowOperator2.Dispose();
        }

        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
            public DateTime EventTime { get; set; }
        }
    }
}
