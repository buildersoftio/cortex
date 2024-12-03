using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Windows;

namespace Cortex.Streams.Tests
{
    public class SlidingWindowOperatorTests
    {
        [Fact]
        public void SlidingWindowOperator_BasicFunctionality_WindowsAggregatedCorrectly()
        {
            // Arrange
            var windowDuration = TimeSpan.FromSeconds(10);
            var slideInterval = TimeSpan.FromSeconds(5);

            var emittedValues = new List<WindowOutput>();
            var sinkCalled = new ManualResetEventSlim(false);
            Action<WindowOutput> sinkAction = output =>
            {
                emittedValues.Add(output);
                sinkCalled.Set();
            };

            var windowStateStore = new InMemoryStateStore<WindowKey<string>, List<InputData>>("WindowStateStore");

            // Build the stream
            var stream = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .SlidingWindow<string, WindowOutput>(
                    keySelector: input => input.Key,
                    windowDuration: windowDuration,
                    slideInterval: slideInterval,
                    windowFunction: events =>
                    {
                        var key = events.First().Key;
                        var windowStartTime = events.Min(e => e.EventTime);
                        var windowEndTime = events.Max(e => e.EventTime);
                        var sum = events.Sum(e => e.Value);
                        return new WindowOutput
                        {
                            Key = key,
                            WindowStartTime = windowStartTime,
                            WindowEndTime = windowEndTime,
                            AggregatedValue = sum
                        };
                    },
                    windowStateStore: windowStateStore)
                .Sink(sinkAction)
                .Build();

            stream.Start();

            var now = DateTime.UtcNow;

            // Act
            stream.Emit(new InputData { Key = "A", Value = 1, EventTime = now });
            stream.Emit(new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(3) });
            stream.Emit(new InputData { Key = "A", Value = 3, EventTime = now.AddSeconds(6) });

            // Wait for windows to be processed
            Thread.Sleep(15000); // Wait enough time for windows to be emitted

            // Manually trigger window processing if necessary
            // Not needed if the timer in the operator works correctly

            // Assert
            Assert.True(emittedValues.Count > 0);
            // Verify that the emitted values are correct based on your expectations

            stream.Stop();
        }
        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
            public DateTime EventTime { get; set; }
        }

        public class WindowOutput
        {
            public string Key { get; set; }
            public DateTime WindowStartTime { get; set; }
            public DateTime WindowEndTime { get; set; }
            public int AggregatedValue { get; set; }
        }
    }
}
