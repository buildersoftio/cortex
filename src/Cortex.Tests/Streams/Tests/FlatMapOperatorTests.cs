using Cortex.Streams;
using Cortex.Streams.Operators;

namespace Cortex.Tests.Streams.Tests
{
    public class CollectingSink<TInput> : ISinkOperator<TInput>
    {
        private readonly List<TInput> _collected = new List<TInput>();
        public IReadOnlyList<TInput> Collected => _collected;

        public void Start() { }
        public void Stop() { }

        public void Process(TInput input)
        {
            _collected.Add(input);
        }
    }


    public class FlatMapOperatorTests
    {
        [Fact]
        public void Stream_FlatMap_SplitsInputIntoMultipleOutputs()
        {
            // Arrange
            var collectingSink = new CollectingSink<string>();

            // Build the stream:
            // Start a stream without a dedicated source, we will just Emit into it.
            var stream = StreamBuilder<string, string>
                .CreateNewStream("TestStream")
                .Stream()
                .FlatMap(line => line.Split(' '))   // Use FlatMap to split a sentence into words
                .Sink(collectingSink)
                .Build();

            stream.Start();

            // Act
            stream.Emit("Hello world from stream");

            // Assert
            Assert.Equal(4, collectingSink.Collected.Count);
            Assert.Contains("Hello", collectingSink.Collected);
            Assert.Contains("world", collectingSink.Collected);
            Assert.Contains("from", collectingSink.Collected);
            Assert.Contains("stream", collectingSink.Collected);

            stream.Stop();
        }

        [Fact]
        public void Stream_FlatMap_EmptyResult_EmitsNoOutput()
        {
            // Arrange
            var collectingSink = new CollectingSink<int>();

            var stream = StreamBuilder<int, int>
                .CreateNewStream("EmptyResultStream")
                .Stream()
                .FlatMap(num => new int[0]) // Always empty
                .Sink(collectingSink)
                .Build();

            stream.Start();

            // Act
            stream.Emit(42);

            // Assert
            Assert.Empty(collectingSink.Collected);

            stream.Stop();
        }

        [Fact]
        public void Stream_FlatMap_NullResult_TreatedAsEmpty()
        {
            // Arrange
            var collectingSink = new CollectingSink<int>();

            var stream = StreamBuilder<int, int>
                .CreateNewStream("NullResultStream")
                .Stream()
                .FlatMap<int>(num => null) // Always null
                .Sink(collectingSink)
                .Build();

            stream.Start();

            // Act
            stream.Emit(10);

            // Assert
            Assert.Empty(collectingSink.Collected);

            stream.Stop();
        }

        [Fact]
        public void Stream_FlatMap_ExceptionInFunction_BubblesUp()
        {
            // Arrange
            var collectingSink = new CollectingSink<int>();

            var stream = StreamBuilder<int, int>
                .CreateNewStream("ExceptionStream")
                .Stream()
                .FlatMap<int>(num => throw new InvalidOperationException("Test exception"))
                .Sink(collectingSink)
                .Build();

            stream.Start();

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => stream.Emit(5));
            Assert.Equal("Test exception", ex.Message);

            stream.Stop();
        }

        [Fact]
        public void Stream_FlatMap_SingleOutputEmittedForEachInput()
        {
            // Arrange
            var collectingSink = new CollectingSink<string>();

            var stream = StreamBuilder<string, string>
                .CreateNewStream("SingleOutputStream")
                .Stream()
                .FlatMap(line => new[] { line.ToUpper() }) // One-to-one mapping but via flatmap
                .Sink(collectingSink)
                .Build();

            stream.Start();

            // Act
            stream.Emit("hello");
            stream.Emit("world");

            // Assert
            Assert.Equal(2, collectingSink.Collected.Count);
            Assert.Contains("HELLO", collectingSink.Collected);
            Assert.Contains("WORLD", collectingSink.Collected);

            stream.Stop();
        }
    }
}
