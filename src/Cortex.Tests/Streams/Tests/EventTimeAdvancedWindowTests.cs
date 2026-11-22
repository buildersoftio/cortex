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
    }
}
