using Cortex.States;
using Cortex.Streams.Operators;

namespace Cortex.Streams.Tests
{
    public class TumblingWindowOperatorTests
    {
        [Fact]
        public void TumblingWindowOperator_WindowsDataCorrectly()
        {
            /// This test might fail, because it is using Thread Sleeps, these do not play well with tests.


            // Arrange
            var windowDuration = TimeSpan.FromSeconds(2);
            var windowStateStore = new InMemoryStateStore<string, List<int>>("TestWindowStateStore");
            var windowResultsStateStore = new InMemoryStateStore<(string, DateTime), int>("TestWindowResultsStore");
            var tumblingWindowOperator = new TumblingWindowOperator<int, string, int>(
                keySelector: x => "key",
                windowDuration: windowDuration,
                windowFunction: data => data.Count(),
                windowStateStore: windowStateStore,
                windowResultsStateStore: windowResultsStateStore
            );

            var outputs = new List<int>();
            var sinkOperator = new SinkOperator<int>(x => outputs.Add(x));
            tumblingWindowOperator.SetNext(sinkOperator);

            // Act
            tumblingWindowOperator.Process(1);
            tumblingWindowOperator.Process(2);
            Thread.Sleep(windowDuration + TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            tumblingWindowOperator.Process(3);
            Thread.Sleep(windowDuration + TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close
            Thread.Sleep(TimeSpan.FromMilliseconds(100)); // Wait for window to close

            // Assert
            Assert.Equal(new[] { 2, 1 }, outputs);
        }
    }
}
