using Cortex.States;
using Cortex.Streams.Operators;
using Moq;

namespace Cortex.Streams.Tests
{
    public class TumblingWindowOperatorTests
    {
        [Fact]
        public void TumblingWindowOperator_WindowsDataCorrectly()
        {
            // Arrange
            var windowDuration = TimeSpan.FromSeconds(5);

            var emittedValues = new List<int>();
            Action<int> sinkAction = output =>
            {
                emittedValues.Add(output);
            };

            var stream = StreamBuilder<int, int>
                .CreateNewStream("EventTimeWindowExample")
                .Stream()
                .TumblingWindow(
                    windowSize: TimeSpan.FromMinutes(2),
                    timestampExtractor: x => DateTime.UtcNow, // Replace with actual event time extraction
                    allowedLateness: TimeSpan.FromSeconds(30))
                .Sink(window => Console.WriteLine($"Window contents: {string.Join(", ", window)}"))
                .Build();

          
            stream.Start();

            // Act
            var input1 = 1;
            stream.Emit(input1);

            var input2 =2;
            stream.Emit(input2);

            // Wait for the window to close
            Thread.Sleep(1000);

            // Assert
            Assert.Single(emittedValues);

            stream.Stop();
        }
    }
}
