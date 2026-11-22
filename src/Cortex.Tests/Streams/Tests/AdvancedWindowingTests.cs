using Cortex.States;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Operators.Windows.Triggers;

namespace Cortex.Streams.Tests
{
    public class AdvancedWindowingTests
    {
        public class InputData
        {
            public string Key { get; set; }
            public int Value { get; set; }
            public DateTime EventTime { get; set; }
        }

        #region Count Trigger Tests

        [Fact]
        public void CountTrigger_FiresAfterSpecifiedCount()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(3)
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Start at the next window boundary: all timestamps fall into one window that cannot close during the test
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks + 1) * windowSize.Ticks, DateTimeKind.Utc);

            // Act - emit 3 items (should trigger)
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });
            windowOperator.Process(new InputData { Key = "A", Value = 3, EventTime = now.AddMilliseconds(200) });

            // Small delay to allow processing
            Thread.Sleep(200);

            // Assert - should have fired once
            Assert.True(emittedResults.Count >= 1);
            var firstResult = emittedResults.First();
            Assert.Equal(3, firstResult.Items.Count);
            Assert.Equal(WindowEmissionType.Early, firstResult.EmissionType);
            Assert.False(firstResult.IsFinal);

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void CountTrigger_FiresMultipleTimesInAccumulatingMode()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(2)
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Start at the next window boundary: all timestamps fall into one window that cannot close during the test
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks + 1) * windowSize.Ticks, DateTimeKind.Utc);

            // Act - emit 4 items (should trigger twice in accumulating mode)
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });
            Thread.Sleep(100);
            windowOperator.Process(new InputData { Key = "A", Value = 3, EventTime = now.AddMilliseconds(200) });
            windowOperator.Process(new InputData { Key = "A", Value = 4, EventTime = now.AddMilliseconds(300) });

            Thread.Sleep(200);

            // Assert - should have two emissions with accumulating items
            Assert.True(emittedResults.Count >= 2);
            // First emission should have 2 items
            Assert.Equal(2, emittedResults[0].Items.Count);
            // Second emission should have all 4 items (accumulating)
            Assert.Equal(4, emittedResults[1].Items.Count);

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region Processing Time Trigger Tests

        [Fact]
        public void ProcessingTimeTrigger_FiresAtInterval()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnProcessingTime(TimeSpan.FromMilliseconds(500))
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act - emit item and wait for time-based trigger
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            
            // Wait for processing time trigger to fire
            Thread.Sleep(700);

            // Assert - should have at least one early emission
            Assert.True(emittedResults.Count >= 1);
            var earlyResult = emittedResults.First();
            Assert.Equal(WindowEmissionType.Early, earlyResult.EmissionType);

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region State Mode Tests

        [Fact]
        public void DiscardingMode_EmitsOnlyNewItemsSinceLastFiring()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(2)
                .Discarding()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Start at the next window boundary: all timestamps fall into one window that cannot close during the test
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks + 1) * windowSize.Ticks, DateTimeKind.Utc);

            // Act - emit 4 items
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });
            Thread.Sleep(100);
            windowOperator.Process(new InputData { Key = "A", Value = 3, EventTime = now.AddMilliseconds(200) });
            windowOperator.Process(new InputData { Key = "A", Value = 4, EventTime = now.AddMilliseconds(300) });

            Thread.Sleep(200);

            // Assert - in discarding mode, second emission should only have 2 new items
            Assert.True(emittedResults.Count >= 2);
            Assert.Equal(2, emittedResults[0].Items.Count);
            Assert.Equal(2, emittedResults[1].Items.Count); // Only new items since last fire

            // Cleanup
            windowOperator.Dispose();
        }

        [Fact]
        public void AccumulatingAndRetractingMode_EmitsRetractions()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(2)
                .AccumulatingAndRetracting()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            // Start at the next window boundary: all timestamps fall into one window that cannot close during the test
            var now = new DateTime((DateTime.UtcNow.Ticks / windowSize.Ticks + 1) * windowSize.Ticks, DateTimeKind.Utc);

            // Act - emit 4 items
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });
            Thread.Sleep(100);
            windowOperator.Process(new InputData { Key = "A", Value = 3, EventTime = now.AddMilliseconds(200) });
            windowOperator.Process(new InputData { Key = "A", Value = 4, EventTime = now.AddMilliseconds(300) });

            Thread.Sleep(200);

            // Assert - should have retractions
            var retractions = emittedResults.Where(r => r.EmissionType == WindowEmissionType.Retraction).ToList();
            Assert.True(retractions.Count >= 1, "Should have at least one retraction");

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region Early Trigger Tests

        [Fact]
        public void EarlyTrigger_EmitsPartialResultsBeforeWindowCloses()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(5);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .WithEarlyTrigger(TimeSpan.FromMilliseconds(300))
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            
            // Wait for early emissions
            Thread.Sleep(800);

            // Assert - should have early emissions
            var earlyEmissions = emittedResults.Where(r => r.EmissionType == WindowEmissionType.Early).ToList();
            Assert.True(earlyEmissions.Count >= 1, "Should have at least one early emission");

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region Composite Trigger Tests

        [Fact]
        public void OrTrigger_FiresWhenEitherConditionMet()
        {
            // Arrange
            var countTrigger = new CountTrigger<InputData>(100); // High count
            var timeTrigger = new ProcessingTimeTrigger<InputData>(TimeSpan.FromMilliseconds(300));
            var orTrigger = countTrigger.Or(timeTrigger);

            var windowSize = TimeSpan.FromSeconds(10);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = new WindowConfiguration<InputData>
            {
                Trigger = orTrigger,
                StateMode = WindowStateMode.Accumulating
            };

            var windowOperator = new AdvancedTumblingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act - emit one item (won't reach count threshold)
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });

            // Wait for time trigger
            Thread.Sleep(500);

            // Assert - should fire due to time trigger even though count not met
            Assert.True(emittedResults.Count >= 1);

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region Trigger Factory Tests

        [Fact]
        public void Triggers_OnCount_CreatesCountTrigger()
        {
            // Act
            var trigger = Triggers.OnCount<InputData>(5);

            // Assert
            Assert.IsType<CountTrigger<InputData>>(trigger);
            Assert.Contains("5", trigger.Description);
        }

        [Fact]
        public void Triggers_OnProcessingTime_CreatesProcessingTimeTrigger()
        {
            // Act
            var trigger = Triggers.OnProcessingTime<InputData>(TimeSpan.FromSeconds(10));

            // Assert
            Assert.IsType<ProcessingTimeTrigger<InputData>>(trigger);
        }

        [Fact]
        public void Triggers_OnCountOrTime_CreatesCombinedTrigger()
        {
            // Act
            var trigger = Triggers.OnCountOrTime<InputData>(5, TimeSpan.FromSeconds(10));

            // Assert
            Assert.IsType<OrTrigger<InputData>>(trigger);
        }

        #endregion

        #region Window Configuration Builder Tests

        [Fact]
        public void WindowConfigurationBuilder_BuildsCorrectConfiguration()
        {
            // Act
            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(5)
                .Accumulating()
                .WithAllowedLateness(TimeSpan.FromSeconds(30))
                .Build();

            // Assert
            Assert.IsType<CountTrigger<InputData>>(config.Trigger);
            Assert.Equal(WindowStateMode.Accumulating, config.StateMode);
            Assert.Equal(TimeSpan.FromSeconds(30), config.AllowedLateness);
        }

        [Fact]
        public void WindowConfigurationBuilder_ChainsTriggers()
        {
            // Act
            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(5)
                .OrTrigger(new ProcessingTimeTrigger<InputData>(TimeSpan.FromSeconds(10)))
                .Build();

            // Assert
            Assert.IsType<OrTrigger<InputData>>(config.Trigger);
        }

        #endregion

        #region Sliding Window Advanced Tests

        [Fact]
        public void AdvancedSlidingWindow_WithCountTrigger_EmitsEarlyResults()
        {
            // Arrange
            var windowSize = TimeSpan.FromSeconds(10);
            var slideInterval = TimeSpan.FromSeconds(2);
            var stateStore = new InMemoryStateStore<string, List<InputData>>("AdvancedSlidingWindowStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(2)
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedSlidingWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                windowSize: windowSize,
                slideInterval: slideInterval,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });

            Thread.Sleep(200);

            // Assert - should have fired for overlapping windows
            Assert.True(emittedResults.Count >= 1);

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region Session Window Advanced Tests

        [Fact]
        public void AdvancedSessionWindow_WithCountTrigger_EmitsEarlyResults()
        {
            // Arrange
            var inactivityGap = TimeSpan.FromSeconds(5);
            var stateStore = new InMemoryStateStore<string, AdvancedSessionState<InputData>>("AdvancedSessionStateStore");
            var emittedResults = new List<WindowResult<string, InputData>>();

            var config = WindowConfiguration<InputData>.Create()
                .TriggerOnCount(2)
                .Accumulating()
                .Build();

            var windowOperator = new AdvancedSessionWindowOperator<InputData, string>(
                keySelector: x => x.Key,
                timestampSelector: x => x.EventTime,
                inactivityGap: inactivityGap,
                stateStore: stateStore,
                config: config);

            var sinkOperator = new SinkOperator<WindowResult<string, InputData>>(result =>
            {
                lock (emittedResults)
                {
                    emittedResults.Add(result);
                }
            });
            windowOperator.SetNext(sinkOperator);

            var now = DateTime.UtcNow;

            // Act
            windowOperator.Process(new InputData { Key = "A", Value = 1, EventTime = now });
            windowOperator.Process(new InputData { Key = "A", Value = 2, EventTime = now.AddMilliseconds(100) });

            Thread.Sleep(200);

            // Assert
            Assert.True(emittedResults.Count >= 1);
            var firstResult = emittedResults.First();
            Assert.Equal(WindowEmissionType.Early, firstResult.EmissionType);

            // Cleanup
            windowOperator.Dispose();
        }

        #endregion

        #region WindowResult Metadata Tests

        [Fact]
        public void WindowResult_ContainsCorrectMetadata()
        {
            // Arrange
            var items = new List<InputData>
            {
                new InputData { Key = "A", Value = 1, EventTime = DateTime.UtcNow }
            };

            // Act
            var result = new WindowResult<string, InputData>(
                "A",
                DateTime.UtcNow.AddSeconds(-10),
                DateTime.UtcNow,
                items,
                WindowEmissionType.Early,
                false,
                DateTime.UtcNow,
                1);

            // Assert
            Assert.Equal("A", result.Key);
            Assert.Equal(WindowEmissionType.Early, result.EmissionType);
            Assert.False(result.IsFinal);
            Assert.Equal(1, result.EmissionSequence);
        }

        [Fact]
        public void WindowResult_AsRetraction_CreatesRetractionResult()
        {
            // Arrange
            var items = new List<InputData>
            {
                new InputData { Key = "A", Value = 1, EventTime = DateTime.UtcNow }
            };
            var original = new WindowResult<string, InputData>(
                "A",
                DateTime.UtcNow.AddSeconds(-10),
                DateTime.UtcNow,
                items,
                WindowEmissionType.Early,
                false,
                DateTime.UtcNow,
                1);

            // Act
            var retraction = original.AsRetraction();

            // Assert
            Assert.Equal(WindowEmissionType.Retraction, retraction.EmissionType);
            Assert.Equal(original.Key, retraction.Key);
        }

        #endregion
    }
}
