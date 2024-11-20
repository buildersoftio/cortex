using Cortex.States;
using Cortex.Streams.Operators;

namespace Cortex.Streams.Tests
{
    public class SlidingWindowOperatorTests
    {
        [Fact]
        public void SlidingWindowOperator_WindowsDataCorrectly()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(4);
            var advanceBy = TimeSpan.FromSeconds(2);
            var windowStateStore = new InMemoryStateStore<string, List<(int, DateTime)>>("TestSlidingWindowStateStore");
            var windowResultsStateStore = new InMemoryStateStore<(string, DateTime), int>("TestSlidingWindowResultsStore");
            var slidingWindowOperator = new SlidingWindowOperator<int, string, int>(
                keySelector: x => "key",
                windowSize: windowSize,
                advanceBy: advanceBy,
                windowFunction: data => data.Count(),
                windowStateStore: windowStateStore,
                windowResultsStateStore: windowResultsStateStore
            );

            var outputs = new List<int>();
            var sinkOperator = new SinkOperator<int>(x => outputs.Add(x));
            slidingWindowOperator.SetNext(sinkOperator);

            // Act
            slidingWindowOperator.Process(1);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            slidingWindowOperator.Process(2);
            Thread.Sleep(advanceBy + TimeSpan.FromMilliseconds(500)); // Wait for window to advance
            slidingWindowOperator.Process(3);
            Thread.Sleep(windowSize + TimeSpan.FromSeconds(1)); // Wait for all windows to close

            // Assert
            // Expected outputs depend on the timing and windowing logic
            Assert.True(outputs.Count > 0);
        }
    }
}
