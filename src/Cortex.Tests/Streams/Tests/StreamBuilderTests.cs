namespace Cortex.Streams.Tests
{
    public class StreamBuilderTests
    {
        [Fact]
        public void StreamBuilder_CreatesAndRunsStreamCorrectly()
        {
            // Arrange
            var receivedData = new List<int>();
            var stream = StreamBuilder<int, int>
                .CreateNewStream("TestStream")
                .Stream()
                .Map(x => x * 2)
                .Filter(x => x > 5)
                .Sink(x => receivedData.Add(x))
                .Build();

            stream.Start();

            // Act
            stream.Emit(1);
            stream.Emit(2);
            stream.Emit(3);
            stream.Emit(4);

            // Assert
            Assert.Equal(new[] { 6, 8 }, receivedData);
        }

        [Fact]
        public void Build_ShouldCreateStreamSuccessfully()
        {
            // Arrange
            var builder = StreamBuilder<int, int>.CreateNewStream("TestStream")
                .Stream()
                .Map(x => x * 2)
                .Filter(x => x > 5);

            // Act
            var stream = builder.Build();

            // Assert
            Assert.NotNull(stream);
        }
    }
}
