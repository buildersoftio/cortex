using Cortex.States;
using Cortex.Streams.Windows;
using Moq;

namespace Cortex.Streams.Tests
{
    public class SessionWindowOperatorTests
    {

        [Fact]
        public void SessionWindowOperator_BasicFunctionality_SessionsAggregatedCorrectly()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(5);

            var emittedValues = new List<SessionOutput>();
            Action<SessionOutput> sinkAction = output =>
            {
                emittedValues.Add(output);
                Console.WriteLine($"Session closed for Key: {output.Key}, Aggregated Value: {output.AggregatedValue}");
            };

            var sessionStateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionStateStore");

            // Build the stream
            var stream = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .SessionWindow<string, SessionOutput>(
                    keySelector: input => input.Key,
                    inactivityGap: inactivityGap,
                    sessionFunction: events =>
                    {
                        var key = events.First().Key;
                        var sum = events.Sum(e => e.Value);
                        var sessionStartTime = events.Min(e => e.EventTime);
                        var sessionEndTime = events.Max(e => e.EventTime);
                        return new SessionOutput
                        {
                            Key = key,
                            AggregatedValue = sum,
                            SessionStartTime = sessionStartTime,
                            SessionEndTime = sessionEndTime
                        };
                    },
                    sessionStateStore: sessionStateStore)
                .Sink(sinkAction)
                .Build();

            stream.Start();

            var now = DateTime.UtcNow;

            // Act
            var input1 = new InputData { Key = "A", Value = 1, EventTime = now };
            stream.Emit(input1);

            Thread.Sleep(2000);

            var input2 = new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(2) };
            stream.Emit(input2);

            Thread.Sleep(6000); // Wait to exceed inactivity gap

            // Wait for session to expire
            Thread.Sleep(1000);

            // Assert
            Assert.Single(emittedValues);
            Assert.Equal("A", emittedValues[0].Key);
            Assert.Equal(3, emittedValues[0].AggregatedValue); // 1 + 2 = 3
            Assert.Equal(now, emittedValues[0].SessionStartTime);
            Assert.Equal(now.AddSeconds(2), emittedValues[0].SessionEndTime);

            stream.Stop();
        }

        [Fact]
        public void SessionWindowOperator_InactivityGap_SessionClosesAfterInactivity()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(2);

            var emittedValues = new List<SessionOutput>();
            Action<SessionOutput> sinkAction = output =>
            {
                emittedValues.Add(output);
                Console.WriteLine($"Session closed for Key: {output.Key}, Aggregated Value: {output.AggregatedValue}");
            };

            var sessionStateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionStateStore");

            // Build the stream
            var stream = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .SessionWindow<string, SessionOutput>(
                    keySelector: input => input.Key,
                    inactivityGap: inactivityGap,
                    sessionFunction: events =>
                    {
                        var key = events.First().Key;
                        var sum = events.Sum(e => e.Value);
                        var sessionStartTime = events.Min(e => e.EventTime);
                        var sessionEndTime = events.Max(e => e.EventTime);
                        return new SessionOutput
                        {
                            Key = key,
                            AggregatedValue = sum,
                            SessionStartTime = sessionStartTime,
                            SessionEndTime = sessionEndTime
                        };
                    },
                    sessionStateStore: sessionStateStore)
                .Sink(sinkAction)
                .Build();

            stream.Start();

            var now = DateTime.UtcNow;

            // Act
            stream.Emit(new InputData { Key = "A", Value = 1, EventTime = now });

            Thread.Sleep(1000);

            stream.Emit(new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(1) });

            Thread.Sleep(3000); // Wait to exceed inactivity gap

            // Wait for session to expire
            Thread.Sleep(1000);

            // Assert
            Assert.Single(emittedValues);
            Assert.Equal(3, emittedValues[0].AggregatedValue);

            stream.Stop();
        }

        [Fact]
        public void SessionWindowOperator_StatePersistence_StateRestoredCorrectly()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(5);

            var emittedValues = new List<SessionOutput>();
            Action<SessionOutput> sinkAction = output =>
            {
                emittedValues.Add(output);
            };

            var sessionStateStore = new InMemoryStateStore<string, SessionState<InputData>>("SessionStateStore");

            // First stream instance
            var stream1 = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .SessionWindow<string, SessionOutput>(
                    keySelector: input => input.Key,
                    inactivityGap: inactivityGap,
                    sessionFunction: events =>
                    {
                        var key = events.First().Key;
                        var sum = events.Sum(e => e.Value);
                        var sessionStartTime = events.Min(e => e.EventTime);
                        var sessionEndTime = events.Max(e => e.EventTime);
                        return new SessionOutput
                        {
                            Key = key,
                            AggregatedValue = sum,
                            SessionStartTime = sessionStartTime,
                            SessionEndTime = sessionEndTime
                        };
                    },
                    sessionStateStore: sessionStateStore)
                .Sink(sinkAction)
                .Build();

            stream1.Start();

            var now = DateTime.UtcNow;

            // Act
            stream1.Emit(new InputData { Key = "A", Value = 1, EventTime = now });

            // Simulate application restart
            stream1.Stop();

            var stream2 = StreamBuilder<InputData, InputData>
                .CreateNewStream("Test Stream")
                .Stream()
                .SessionWindow<string, SessionOutput>(
                    keySelector: input => input.Key,
                    inactivityGap: inactivityGap,
                    sessionFunction: events =>
                    {
                        var key = events.First().Key;
                        var sum = events.Sum(e => e.Value);
                        var sessionStartTime = events.Min(e => e.EventTime);
                        var sessionEndTime = events.Max(e => e.EventTime);
                        return new SessionOutput
                        {
                            Key = key,
                            AggregatedValue = sum,
                            SessionStartTime = sessionStartTime,
                            SessionEndTime = sessionEndTime
                        };
                    },
                    sessionStateStore: sessionStateStore)
                .Sink(sinkAction)
                .Build();

            stream2.Start();

            stream2.Emit(new InputData { Key = "A", Value = 2, EventTime = now.AddSeconds(2) });

            // Wait to exceed inactivity gap
            Thread.Sleep(6000);

            // Wait for session to expire
            Thread.Sleep(1000);

            // Assert
            Assert.Single(emittedValues);
            Assert.Equal(3, emittedValues[0].AggregatedValue);

            stream2.Stop();
        }

        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
            public DateTime EventTime { get; set; }
        }

        public class SessionOutput
        {
            public string Key { get; set; }
            public int AggregatedValue { get; set; }
            public DateTime SessionStartTime { get; set; }
            public DateTime SessionEndTime { get; set; }
        }
    }
}
