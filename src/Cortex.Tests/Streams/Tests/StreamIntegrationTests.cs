namespace Cortex.Streams.Tests
{
    public class StreamIntegrationTests
    {
        [Fact]
        public void FullPipeline_ShouldProcessDataCorrectly()
        {
            // Arrange
            string result = null;

            var stream = StreamBuilder<int, int>.CreateNewStream("TestStream")
                .Stream()
                .Filter(x => x > 5)
                .Map(x => x * 2)
                .Sink(x => result = $"Result: {x}")
                .Build();

            // Act
            stream.Start();
            stream.Emit(7);

            // Assert
            Assert.Equal("Result: 14", result);
        }

        [Fact]
        public void Pipeline_ShouldFilterOutInvalidData()
        {
            // Arrange
            string result = null;

            var stream = StreamBuilder<int, int>.CreateNewStream("TestStream")
                .Stream()
                .Filter(x => x > 10)
                .Sink(x => result = $"Result: {x}")
                .Build();

            // Act
            stream.Start();
            stream.Emit(5);

            // Assert
            Assert.Null(result);
        }
    }
}
