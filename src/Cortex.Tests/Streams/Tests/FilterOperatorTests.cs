using Cortex.Streams.Operators;
using Moq;

namespace Cortex.Streams.Tests
{
    public class FilterOperatorTests
    {
        [Fact]
        public void FilterOperator_FiltersOutUnwantedData()
        {
            // Arrange
            var filterOperator = new FilterOperator<int>(x => x % 2 == 0);
            var inputs = new[] { 1, 2, 3, 4, 5 };
            var expectedOutputs = new[] { 2, 4 };
            var actualOutputs = new List<int>();

            var sinkOperator = new SinkOperator<int>(x => actualOutputs.Add(x));
            filterOperator.SetNext(sinkOperator);

            // Act
            foreach (var input in inputs)
            {
                filterOperator.Process(input);
            }

            // Assert
            Assert.Equal(expectedOutputs, actualOutputs);
        }

        [Fact]
        public void Process_ShouldPassDataWhenPredicateIsTrue()
        {
            // Arrange
            var filterOperator = new FilterOperator<int>(x => x > 5);
            var nextOperator = new Mock<IOperator>();
            filterOperator.SetNext(nextOperator.Object);

            // Act
            filterOperator.Process(10);

            // Assert
            nextOperator.Verify(op => op.Process(10), Times.Once);
        }

        [Fact]
        public void Process_ShouldNotPassDataWhenPredicateIsFalse()
        {
            // Arrange
            var filterOperator = new FilterOperator<int>(x => x > 5);
            var nextOperator = new Mock<IOperator>();
            filterOperator.SetNext(nextOperator.Object);

            // Act
            filterOperator.Process(3);

            // Assert
            nextOperator.Verify(op => op.Process(It.IsAny<int>()), Times.Never);
        }
    }
}
