using Cortex.Streams;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// End to end: a source replays sensor readings out of order through WithWatermarks, a Map and an event-time
    /// tumbling window (issue #197). One reading is too late, and the last window closes only because the source goes idle.
    /// </summary>
    public class EventTimeEndToEndTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Reading(string SensorId, DateTime MeasuredAt, double Celsius);

        private sealed record Fahrenheit(string SensorId, DateTime MeasuredAt, double Value);

        /// <summary>Source that emits only when the test pushes a reading.</summary>
        private sealed class ReplaySource : ISourceOperator<Reading>
        {
            private Action<Reading> _emit;

            public void Start(Action<Reading> emit) => _emit = emit;

            public void Stop() => _emit = null;

            public void Push(Reading reading) => _emit?.Invoke(reading);
        }

        [Fact]
        public void ReplayedReadings_AreWindowedByEventTime_LateOneReported_IdleTailClosed()
        {
            var now = new DateTime(2026, 11, 22, 12, 0, 0, DateTimeKind.Utc);
            var clockLock = new object();
            var results = new List<WindowResult<string, Fahrenheit>>();
            var late = new List<LateEvent>();
            var source = new ReplaySource();

            var strategy = WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5))
                .WithIdleTimeout(TimeSpan.FromMilliseconds(100))
                .WithClock(() => { lock (clockLock) return now; })
                .OnLateEvent(e => { lock (late) late.Add(e); });

            var stream = StreamBuilder<Reading>
                .CreateNewStream("SensorReplay")
                .Stream(source)
                .WithWatermarks(r => r.MeasuredAt, strategy)
                .Map(r => new Fahrenheit(r.SensorId, r.MeasuredAt, r.Celsius * 9 / 5 + 32))
                .TumblingWindow(f => f.SensorId, f => f.MeasuredAt, TimeSpan.FromSeconds(10))
                .Sink(w => { lock (results) results.Add(w); })
                .Build();
            stream.Start();

            void Push(int seconds, double celsius) => source.Push(new Reading("s1", T0.AddSeconds(seconds), celsius));

            Push(1, 10); Push(4, 11); Push(8, 12);
            Push(3, 13);    // out of order, W = 3
            Push(12, 14);   // W = 7
            Push(9, 15);    // out of order but [0,10) still open
            Push(17, 16);   // W = 12: [0,10) fires
            Push(2, 17);    // too late for [0,10)
            Push(22, 18);   // W = 17
            Push(15, 19);   // out of order, [10,20) still open
            Push(26, 20);   // W = 21: [10,20) fires
            Push(28, 21);   // W = 23: [20,30) stays open

            lock (results)
            {
                Assert.Equal(2, results.Count);
                Assert.Equal(new[] { 50.0, 51.8, 53.6, 55.4, 59.0 }, results[0].Items.Select(f => f.Value));
                Assert.Equal(new[] { 57.2, 60.8, 66.2 }, results[1].Items.Select(f => f.Value));
            }

            var dropped = Assert.Single(late);
            Assert.Equal(T0.AddSeconds(2), dropped.Timestamp);
            Assert.Equal(T0.AddSeconds(12), dropped.Watermark);
            Assert.IsType<Fahrenheit>(dropped.Item);

            // The source goes quiet while the clock keeps running: the idle timer moves the watermark past 30 s
            Assert.True(SpinWait.SpinUntil(() =>
            {
                lock (clockLock) now = now.AddSeconds(5);
                Thread.Sleep(30);
                lock (results) return results.Count == 3;
            }, TimeSpan.FromSeconds(10)));

            lock (results)
            {
                Assert.Equal(T0.AddSeconds(20), results[2].WindowStart);
                Assert.Equal(new[] { 64.4, 68.0, 69.8 }, results[2].Items.Select(f => f.Value));
            }

            stream.Stop();
        }
    }
}
