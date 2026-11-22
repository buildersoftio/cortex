using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Windows;
using Moq;

namespace Cortex.Streams.Tests
{
    public class TumblingWindowOperatorTests
    {
        [Fact]
        public void TumblingWindowOperator_GroupsItemsIntoWindows()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new TumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Use a base time aligned to window boundary
            var now = DateTime.UtcNow;
            var windowTicks = windowSize.Ticks;
            var alignedStart = new DateTime((now.Ticks / windowTicks) * windowTicks, DateTimeKind.Utc);

            // Act - emit items within the same window
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = alignedStart.AddMilliseconds(100) });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = alignedStart.AddMilliseconds(500) });

            // Wait for window to close
            Thread.Sleep(3000);

            // Assert - should have at least one window result with all items
            Assert.True(emittedResults.Count >= 1);
            var totalItems = emittedResults.SelectMany(r => r.Items).ToList();
            Assert.Equal(2, totalItems.Count);
            Assert.Equal(3, totalItems.Sum(x => x.Value));

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void TumblingWindowOperator_SeparatesItemsByKey()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new TumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Start at a window boundary so both timestamps fall into the same window
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks) * windowSize.Ticks, DateTimeKind.Utc);

            // Act - emit items for different keys
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "B", Value = 10, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(500) });
            windowOperator.Process(new InputData { Key = "B", Value = 20, EventTime = now.AddMilliseconds(500) });

            // Wait for windows to close
            Thread.Sleep(2500);

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
        public void TumblingWindowOperator_CreatesNewWindowAfterPreviousClosed()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(1);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var windowOperator = new TumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
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

            // Act - first window
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });

            // Wait for first window to close
            Thread.Sleep(1500);

            // Second window
            windowOperator.Process(new InputData { Key = "A", Value = 5, EventTime = now.AddSeconds(2) });

            // Wait for second window to close
            Thread.Sleep(1500);

            // Assert
            Assert.Equal(2, emittedResults.Count);
            Assert.Equal(1, emittedResults[0].Items.Sum(x => x.Value));
            Assert.Equal(5, emittedResults[1].Items.Sum(x => x.Value));

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void TumblingWindowOperator_ThrowsOnNullKeySelector()
        {
            // Arrange & Act & Assert
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");

            Assert.Throws<ArgumentNullException>(() =>
                new TumblingWindowOperator<InputData, string>(
                    keySelector: null,
                    timestampSelector: x => x.EventTime,
                    windowSize: TimeSpan.FromSeconds(1),
                    stateStore: stateStore));
        }

        [Fact]
        public void TumblingWindowOperator_ThrowsOnNullTimestampSelector()
        {
            // Arrange & Act & Assert
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");

            Assert.Throws<ArgumentNullException>(() =>
                new TumblingWindowOperator<InputData, string>(
                    keySelector: x => x.Key,
                    timestampSelector: null,
                    windowSize: TimeSpan.FromSeconds(1),
                    stateStore: stateStore));
        }

        [Fact]
        public void TumblingWindowOperator_ThrowsOnNullStateStore()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TumblingWindowOperator<InputData, string>(
                    keySelector: x => x.Key,
                    timestampSelector: x => x.EventTime,
                    windowSize: TimeSpan.FromSeconds(1),
                    stateStore: null));
        }

        [Fact]
        public void TumblingWindowOperator_ThreadSafety_NoExceptionsThrown()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("TumblingWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();
            var lockObj = new object();

            var windowOperator = new TumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
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
            for (int i = 0; i < 100; i++)
            {
                int value = i;
                tasks.Add(Task.Run(() =>
                {
                    windowOperator.Process(new InputData { Key = "A", Value = value, EventTime = now });
                }));
            }

            Task.WaitAll(tasks.ToArray());

            // Wait for window to close
            Thread.Sleep(2500);

            // Assert
            Assert.True(emittedResults.Count >= 1);
            int totalSum = emittedResults.SelectMany(r => r.Items).Sum(x => x.Value);
            int expectedSum = Enumerable.Range(0, 100).Sum();
            Assert.Equal(expectedSum, totalSum);

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void TumblingWindowOperator_IntegrationWithStreamBuilder()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(2);
            var emittedResults = new List<WindowResult<string, InputData>>();

            var stream = StreamBuilder<InputData>
                .CreateNewStream("Test Tumbling Window Stream")
                .Stream()
                .TumblingWindow<string>(
                    keySelector: x => x.Key,
                    timestampSelector: x => x.EventTime,
                    windowSize: windowSize)
                .Sink(result => emittedResults.Add(result))
                .Build();

            stream.Start();

            // Start at a window boundary so both timestamps fall into the same window
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks) * windowSize.Ticks, DateTimeKind.Utc);

            // Act
            stream.Emit(new InputData { Key = "A", Value = 1, EventTime = now });
            stream.Emit(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(500) });

            // Wait for window to close
            Thread.Sleep(2500);

            // Assert
            Assert.Single(emittedResults);
            Assert.Equal(2, emittedResults[0].Items.Count);
            Assert.Equal(3, emittedResults[0].Items.Sum(x => x.Value));

            stream.Stop();
        }

        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
            public DateTime EventTime { get; set; }
        }
    }
}
