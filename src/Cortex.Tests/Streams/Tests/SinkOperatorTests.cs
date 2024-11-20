using Cortex.Streams.Operators;

namespace Cortex.Streams.Tests
{
    public class SinkOperatorTests
    {
        [Fact]
        public void SinkOperator_ReceivesData()
        {
            // Arrange
            var receivedData = new List<int>();
            var sinkOperator = new SinkOperator<int>(x => receivedData.Add(x));

            // Act
            sinkOperator.Process(1);
            sinkOperator.Process(2);
            sinkOperator.Process(3);

            // Assert
            Assert.Equal(new[] { 1, 2, 3 }, receivedData);
        }

        [Fact]
        public void Process_ShouldConsumeDataUsingSinkFunction()
        {
            // Arrange
            string result = null;
            var sinkOperator = new SinkOperator<string>(data => result = data);

            // Act
            sinkOperator.Process("Test Data");

            // Assert
            Assert.Equal("Test Data", result);
        }

        [Fact]
        public void Process_ShouldNotThrowForValidInput()
        {
            // Arrange
            var sinkOperator = new SinkOperator<int>(data => { /* consume data */ });

            // Act & Assert
            var exception = Record.Exception(() => sinkOperator.Process(42));
            Assert.Null(exception);
        }
    }
}
