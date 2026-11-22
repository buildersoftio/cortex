using Cortex.Streams;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Event-time advanced windows with triggers (issue #197). Timestamps are in 2020, so any result that a
    /// wall-clock trigger produced would appear at once.
    /// </summary>
    public class EventTimeAdvancedWindowTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Ev(string Key, DateTime Time, int Value);

        private static Ev At(double seconds, int value, string key = "a") => new Ev(key, T0.AddSeconds(seconds), value);

        private static int[] Values(WindowResult<string, Ev> window) => window.Items.Select(e => e.Value).ToArray();

        private static IStream<Ev> AdvancedTumbling(
            WatermarkStrategy strategy, WindowConfiguration<Ev> config, List<WindowResult<string, Ev>> results)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeAdvancedTumbling")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .AdvancedTumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), config)
                .Sink(r => { lock (results) results.Add(r); })
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void DefaultTrigger_FiresWhenTheWatermarkReachesTheWindowEnd_NotOnTheWallClock()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = AdvancedTumbling(WatermarkStrategy.ForMonotonousTimestamps(), new WindowConfiguration<Ev>(), results);

            stream.Emit(At(1, 1));
            stream.Emit(At(5, 2));
            Thread.Sleep(300);   // the wall-clock timer runs every 100 ms
            Assert.Empty(results);

            stream.Emit(At(12, 3));   // W = 12

            var window = Assert.Single(results);
            Assert.Equal(new[] { 1, 2 }, Values(window));
            Assert.Equal(WindowEmissionType.OnTime, window.EmissionType);
            Assert.True(window.IsFinal);
        }

        [Fact]
        public void CountTrigger_FiresOnCount_AndPurgesAtTheWatermark()
        {
            var results = new List<WindowResult<string, Ev>>();
            var config = WindowConfiguration<Ev>.Create().TriggerOnCount(2).Accumulating().Build();
            var stream = AdvancedTumbling(WatermarkStrategy.ForMonotonousTimestamps(), config, results);

            stream.Emit(At(1, 1));
            stream.Emit(At(2, 2));    // count reached: early result
            stream.Emit(At(3, 3));
            stream.Emit(At(11, 4));   // W = 11: final result

            Assert.Equal(new[] { new[] { 1, 2 }, new[] { 1, 2, 3 } }, results.Select(Values));
            Assert.Equal(WindowEmissionType.Early, results[0].EmissionType);
            Assert.Equal(WindowEmissionType.OnTime, results[1].EmissionType);
            Assert.True(results[1].IsFinal);
        }

        [Fact]
        public void WindowConfigurationLateness_TakesPrecedence_AndAllowedLateEventsFireAsLate()
        {
            var configLate = new List<int>();
            var strategyLate = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var config = WindowConfiguration<Ev>.Create()
                .WithAllowedLateness(TimeSpan.FromSeconds(10))
                .OnLateEvent((e, _) => configLate.Add(e.Value))
                .Build();
            var strategy = WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(strategyLate.Add);
            var stream = AdvancedTumbling(strategy, config, results);

            stream.Emit(At(1, 1));
            stream.Emit(At(12, 2));   // W = 12: [0,10) fires on time
            stream.Emit(At(4, 3));    // within 10 s lateness: fires at once as a late result
            stream.Emit(At(25, 4));   // W = 25: [10,20) fires, [0,10) expires
            stream.Emit(At(5, 5));    // past the lateness: the configuration's callback, not the strategy's

            Assert.Equal(new[] { new[] { 1 }, new[] { 3 }, new[] { 2 } }, results.Select(Values));
            Assert.Equal(
                new[] { WindowEmissionType.OnTime, WindowEmissionType.Late, WindowEmissionType.OnTime },
                results.Select(r => r.EmissionType));
            Assert.Equal(new[] { 5 }, configLate);
            Assert.Empty(strategyLate);
        }

        [Fact]
        public void AdvancedSliding_FiresEachWindowByWatermark_AndReportsALateEventOnce()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeAdvancedSliding")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add))
                .AdvancedSlidingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), new WindowConfiguration<Ev>())
                .Sink(r => { lock (results) results.Add(r); })
                .Build();
            stream.Start();

            stream.Emit(At(7, 1));    // [0,10) and [5,15)
            stream.Emit(At(12, 2));   // W = 12: [0,10) fires
            stream.Emit(At(3, 3));    // [-5,5) and [0,10): both expired
            stream.Emit(At(30, 4));   // W = 30: [5,15) and [10,20) fire

            Assert.Equal(
                new[] { (0, new[] { 1 }), (5, new[] { 1, 2 }), (10, new[] { 2 }) },
                results.Select(r => ((int)(r.WindowStart - T0).TotalSeconds, Values(r))));
            Assert.Equal(3, ((Ev)Assert.Single(late).Item).Value);
        }

        [Fact]
        public void Accumulating_KeepsItemsThroughTheAllowedLateness_SoACorrectionHasThemAll()
        {
            var results = new List<WindowResult<string, Ev>>();
            var config = WindowConfiguration<Ev>.Create().Accumulating().WithAllowedLateness(TimeSpan.FromSeconds(10)).Build();
            var stream = AdvancedTumbling(WatermarkStrategy.ForMonotonousTimestamps(), config, results);

            stream.Emit(At(1, 1));
            stream.Emit(At(12, 2));   // [0,10) fires on time with [1]
            stream.Emit(At(4, 3));    // allowed late: the correction has [1, 3]

            Assert.Equal(new[] { new[] { 1 }, new[] { 1, 3 } }, results.Select(Values));
            Assert.Equal(WindowEmissionType.Late, results[1].EmissionType);
        }

        [Fact]
        public void WindowWhoseTriggerNeverFires_IsRemovedAfterTheLateness()
        {
            var store = new Cortex.States.InMemoryStateStore<string, List<Ev>>("never-fires");
            var config = WindowConfiguration<Ev>.Create()
                .WithTrigger(new Cortex.Streams.Operators.Windows.Triggers.CustomTrigger<Ev>(
                    onProcessingTime: (time, start, end, context) => Cortex.Streams.Operators.Windows.Triggers.TriggerResult.Continue))
                .Build();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("NeverFires")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .AdvancedTumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), config, stateStore: store)
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(At(1, 1));
            stream.Emit(At(30, 2, "b"));   // W = 30: [0,10) of key a is past its lateness

            Assert.DoesNotContain(store.GetKeys(), k => k.StartsWith("a_"));
        }

        [Fact]
        public void ConfiguredLateCallbackThatThrows_IsIgnoredAndCounted()
        {
            var config = WindowConfiguration<Ev>.Create()
                .OnLateEvent((e, _) => throw new InvalidOperationException("callback failure"))
                .Build();
            var stream = AdvancedTumbling(WatermarkStrategy.ForMonotonousTimestamps(), config, new List<WindowResult<string, Ev>>());

            stream.Emit(At(1, 1));
            stream.Emit(At(20, 2));
            var exception = Record.Exception(() => stream.Emit(At(2, 3)));   // late

            Assert.Null(exception);
        }

        [Fact]
        public void AdvancedSession_DoesNotCloseAtExactlyLastPlusGap()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = AdvancedSessions(WatermarkStrategy.ForMonotonousTimestamps(), new WindowConfiguration<Ev>(), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(10, 2, "b"));   // W = 10 = 0 + gap: an event at 10 could still join
            Assert.Empty(results);

            stream.Emit(At(10, 3));        // joins the session of key a
            stream.Emit(At(30, 4, "b"));

            Assert.Equal(new[] { 1, 3 }, Values(results.Single(r => r.Key == "a")));
        }

        private static IStream<Ev> AdvancedSessions(
            WatermarkStrategy strategy, WindowConfiguration<Ev> config, List<WindowResult<string, Ev>> results)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeAdvancedSessions")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .AdvancedSessionWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), config)
                .Sink(r => { lock (results) results.Add(r); })
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void AdvancedSession_MergesOutOfOrderEvents_AndClosesByWatermark()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = AdvancedSessions(
                WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(20)), new WindowConfiguration<Ev>(), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(18, 2));   // a second session
            stream.Emit(At(9, 3));    // bridges both
            Thread.Sleep(300);        // the wall-clock timer runs every 100 ms
            Assert.Empty(results);

            stream.Emit(At(60, 4));   // W = 40

            var session = Assert.Single(results);
            Assert.Equal(new[] { 1, 2, 3 }, Values(session));
            Assert.Equal(T0, session.WindowStart);
            Assert.Equal(T0.AddSeconds(28), session.WindowEnd);
            Assert.Equal(WindowEmissionType.OnTime, session.EmissionType);
            Assert.True(session.IsFinal);
        }

        [Fact]
        public void AdvancedSession_HonoursConfiguredLateness_AndLateCallback()
        {
            var configLate = new List<int>();
            var results = new List<WindowResult<string, Ev>>();
            var config = WindowConfiguration<Ev>.Create()
                .WithAllowedLateness(TimeSpan.FromSeconds(10))
                .OnLateEvent((e, _) => configLate.Add(e.Value))
                .Build();
            var stream = AdvancedSessions(WatermarkStrategy.ForMonotonousTimestamps(), config, results);

            stream.Emit(At(0, 1));
            stream.Emit(At(15, 2, "b"));   // W = 15: session a fires, kept until W > 20
            stream.Emit(At(3, 3));         // joins the fired session: a late result
            stream.Emit(At(40, 4, "b"));   // W = 40: a expires, b's first session fires
            stream.Emit(At(4, 5));         // past the lateness

            Assert.Equal(new[] { new[] { 1 }, new[] { 3 }, new[] { 2 } }, results.Select(Values));
            Assert.Equal(
                new[] { WindowEmissionType.OnTime, WindowEmissionType.Late, WindowEmissionType.OnTime },
                results.Select(r => r.EmissionType));
            Assert.Equal(new[] { 5 }, configLate);
        }
    }
}
