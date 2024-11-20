using Cortex.Streams.Operators;
using Moq;

namespace Cortex.Streams.Tests
{
    public class MapOperatorTests
    {
        [Fact]
        public void MapOperator_TransformsInputCorrectly()
        {
            // Arrange
            var mapOperator = new MapOperator<int, int>(x => x * 2);
            int input = 5;
            int expectedOutput = 10;
            int actualOutput = 0;

            var sinkOperator = new SinkOperator<int>(x => actualOutput = x);
            mapOperator.SetNext(sinkOperator);

            // Act
            mapOperator.Process(input);

            // Assert
            Assert.Equal(expectedOutput, actualOutput);
        }

        [Fact]
        public void MapOperator_ThrowsException_OnNullInput()
        {
            // Arrange
            var mapOperator = new MapOperator<string, int>(s => s.Length);
            string input = null;
            int actualOutput = 0;

            var sinkOperator = new SinkOperator<int>(x => actualOutput = x);
            mapOperator.SetNext(sinkOperator);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => mapOperator.Process(input));
        }


        [Fact]
        public void Process_ShouldTransformDataUsingMapFunction()
        {
            // Arrange
            var mapOperator = new MapOperator<int, string>(x => $"Number: {x}");
            var nextOperator = new Mock<IOperator>();
            mapOperator.SetNext(nextOperator.Object);

            // Act
            mapOperator.Process(7);

            // Assert
            nextOperator.Verify(op => op.Process("Number: 7"), Times.Once);
        }

        [Fact]
        public void Process_ShouldThrowIfInputIsInvalid()
        {
            // Arrange
            var mapOperator = new MapOperator<int, string>(x => $"Number: {x}");

            // Act & Assert
            Assert.Throws<InvalidCastException>(() => mapOperator.Process("invalid"));
        }
    }
}
