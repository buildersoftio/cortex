using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Windows;
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

            var windowStateStore = new InMemoryStateStore<string, WindowState<InputData>>("WindowStateStore");
            var windowResultsStateStore = new InMemoryStateStore<WindowKey<string>, int>("WindowResultsStateStore");

            // Build the stream
            var stream = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .TumblingWindow<string, int>(
                    keySelector: input => input.Key,
                    windowDuration: windowDuration,
                    windowFunction: events => events.Sum(e => e.Value),
                    windowStateStore: windowStateStore,
                    windowResultsStateStore: windowResultsStateStore)
                .Sink(sinkAction)
                .Build();

            stream.Start();

            // Act
            var input1 = new InputData { Key = "A", Value = 1 };
            stream.Emit(input1);

            var input2 = new InputData { Key = "A", Value = 2 };
            stream.Emit(input2);

            // Wait for the window to close
            Thread.Sleep(6000);

            // Assert
            Assert.Single(emittedValues);
            Assert.Equal(3, emittedValues[0]); // 1 + 2 = 3

            stream.Stop();
        }

        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
        }


        [Fact]
        public void TumblingWindowOperator_ThreadSafety_NoExceptionsThrown_StreamBuilder()
        {
            // Arrange
            var windowDuration = TimeSpan.FromSeconds(2);

            var emittedValues = new List<int>();
            object emittedValuesLock = new object();
            Action<int> sinkAction = output =>
            {
                lock (emittedValuesLock)
                {
                    emittedValues.Add(output);
                }
            };

            var windowStateStore = new InMemoryStateStore<string, WindowState<InputData>>("WindowStateStore");

            // Build the stream
            var stream = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .TumblingWindow<string, int>(
                    keySelector: input => input.Key,
                    windowDuration: windowDuration,
                    windowFunction: events => events.Sum(e => e.Value),
                    windowStateStore: windowStateStore)
                .Sink(sinkAction)
                .Build();

            stream.Start();

            // Act
            var tasks = new List<Task>();
            for (int i = 0; i < 100; i++)
            {
                int value = i;
                tasks.Add(Task.Run(() =>
                {
                    var input = new InputData { Key = "A", Value = value };
                    stream.Emit(input);
                }));
            }

            Task.WaitAll(tasks.ToArray());

            System.Threading.Thread.Sleep(3000); // Wait for windows to close

            // Assert
            Assert.True(emittedValues.Count > 0);
            int totalInputSum = Enumerable.Range(0, 100).Sum();
            int totalEmittedSum;
            lock (emittedValuesLock)
            {
                totalEmittedSum = emittedValues.Sum();
            }
            Assert.Equal(totalInputSum, totalEmittedSum);

            stream.Stop();
        }

        [Fact]
        public void TumblingWindowOperator_StatePersistence_StateRestoredCorrectly_StreamBuilder()
        {
            // Arrange
            var windowDuration = TimeSpan.FromSeconds(5);

            var emittedValues = new List<int>();
            Action<int> sinkAction = output =>
            {
                emittedValues.Add(output);
            };

            var windowStateStore = new InMemoryStateStore<string, WindowState<InputData>>("WindowStateStore");

            // Build the first stream
            var stream1 = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .TumblingWindow<string, int>(
                    keySelector: input => input.Key,
                    windowDuration: windowDuration,
                    windowFunction: events => events.Sum(e => e.Value),
                    windowStateStore: windowStateStore)
                .Sink(sinkAction)
                .Build();

            stream1.Start();

            // Act
            stream1.Emit(new InputData { Key = "A", Value = 1 });

            // Simulate application restart by creating a new stream with the same state store
            stream1.Stop();

            var stream2 = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .TumblingWindow<string, int>(
                    keySelector: input => input.Key,
                    windowDuration: windowDuration,
                    windowFunction: events => events.Sum(e => e.Value),
                    windowStateStore: windowStateStore)
                .Sink(sinkAction)
                .Build();

            stream2.Start();

            stream2.Emit(new InputData { Key = "A", Value = 2 });

            System.Threading.Thread.Sleep(6000); // Wait for window to close

            // Assert
            Assert.Single(emittedValues);
            Assert.Equal(3, emittedValues[0]); // 1 + 2 = 3

            stream2.Stop();
        }
    }
}
