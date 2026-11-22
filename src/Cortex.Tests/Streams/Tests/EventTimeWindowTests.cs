using Cortex.Streams;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Event-time tumbling and sliding windows driven by watermarks (issue #197).
    /// Timestamps are in 2020, so a wall-clock window would close at once: every result here comes from the watermark.
    /// </summary>
    public class EventTimeWindowTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Ev(string Key, DateTime Time, int Value);

        private static Ev At(int seconds, int value, string key = "a") => new Ev(key, T0.AddSeconds(seconds), value);

        private static int[] Values(WindowResult<string, Ev> window) => window.Items.Select(e => e.Value).ToArray();

        private static IStream<Ev> Tumbling(WatermarkStrategy strategy, List<WindowResult<string, Ev>> results)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeTumbling")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                .Sink(results.Add)
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void Tumbling_OutOfOrderEventsWithinBound_LandInTheirWindows_FiredByWatermark()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Tumbling(WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5)), results);

            stream.Emit(At(1, 1));
            stream.Emit(At(7, 2));
            stream.Emit(At(3, 3));    // out of order, W = 2
            stream.Emit(At(12, 4));   // W = 7
            stream.Emit(At(9, 5));    // out of order, window [0,10) still open
            Assert.Empty(results);

            stream.Emit(At(16, 6));   // W = 11: [0,10) fires
            var first = Assert.Single(results);
            Assert.Equal("a", first.Key);
            Assert.Equal(T0, first.WindowStart);
            Assert.Equal(T0.AddSeconds(10), first.WindowEnd);
            Assert.Equal(new[] { 1, 2, 3, 5 }, Values(first));

            stream.Emit(At(25, 7));   // W = 20: [10,20) fires
            Assert.Equal(2, results.Count);
            Assert.Equal(T0.AddSeconds(10), results[1].WindowStart);
            Assert.Equal(new[] { 4, 6 }, Values(results[1]));
        }

        [Fact]
        public void Tumbling_EventAfterLatenessEnded_IsDroppedAndReported_NotANewWindow()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var stream = Tumbling(WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add), results);

            stream.Emit(At(1, 1));
            stream.Emit(At(20, 2));   // W = 20: [0,10) fires
            stream.Emit(At(5, 3));    // late for [0,10)
            stream.Emit(At(40, 4));   // W = 40: [20,30) fires

            Assert.Equal(new[] { new[] { 1 }, new[] { 2 } }, results.Select(Values));
            var dropped = Assert.Single(late);
            Assert.Equal(3, ((Ev)dropped.Item).Value);
            Assert.Equal(T0.AddSeconds(5), dropped.Timestamp);
            Assert.Equal(T0.AddSeconds(20), dropped.Watermark);
            Assert.Contains("TumblingWindowOperator", dropped.OperatorName);
        }

        [Fact]
        public void Tumbling_LateEventIsDetected_EvenWhenNoWatermarkWasSentToTheWindow()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var stream = Tumbling(WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add), results);

            stream.Emit(At(100, 1));          // window [100,110): nothing is due until W = 110
            stream.Emit(At(108, 2));          // W = 108 is not sent to the window
            stream.Emit(At(5, 3, "b"));       // [0,10) ended long before 108: late

            Assert.Equal(3, ((Ev)Assert.Single(late).Item).Value);
            Assert.Equal(T0.AddSeconds(108), late[0].Watermark);
            Assert.Empty(results);
        }

        [Fact]
        public void Tumbling_AllowedLateness_RefiresWithAllItems_ThenExpires()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var strategy = WatermarkStrategy.ForMonotonousTimestamps()
                .WithAllowedLateness(TimeSpan.FromSeconds(10))
                .OnLateEvent(late.Add);
            var stream = Tumbling(strategy, results);

            stream.Emit(At(1, 1));
            stream.Emit(At(16, 2));   // W = 16: [0,10) fires, kept until W >= 20
            stream.Emit(At(4, 3));    // allowed late: [0,10) fires again
            stream.Emit(At(26, 4));   // W = 26: [0,10) expires, [10,20) fires
            stream.Emit(At(5, 5));    // lateness over: dropped

            Assert.Equal(new[] { new[] { 1 }, new[] { 1, 3 }, new[] { 2 } }, results.Select(Values));
            Assert.Equal(5, ((Ev)Assert.Single(late).Item).Value);
        }

        [Fact]
        public void Tumbling_WallClockTimerNoLongerClosesWindows()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Tumbling(WatermarkStrategy.ForMonotonousTimestamps(), results);

            stream.Emit(At(1, 1));
            Thread.Sleep(300);   // the processing-time timer runs every 100 ms

            Assert.Empty(results);
        }

        [Fact]
        public void Sliding_ChecksEachWindowSeparately_AndReportsAPartlyLateEventOnce()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeSliding")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add))
                .SlidingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5))
                .Sink(results.Add)
                .Build();
            stream.Start();

            stream.Emit(At(7, 1));    // [0,10) and [5,15)
            stream.Emit(At(12, 2));   // W = 12: [0,10) fires
            stream.Emit(At(3, 3));    // [-5,5) and [0,10): both closed
            stream.Emit(At(8, 4));    // [0,10) closed, [5,15) open
            stream.Emit(At(30, 5));   // W = 30: [5,15) and [10,20) fire

            Assert.Equal(
                new[] { (0, new[] { 1 }), (5, new[] { 1, 2, 4 }), (10, new[] { 2 }) },
                results.Select(r => ((int)(r.WindowStart - T0).TotalSeconds, Values(r))));
            Assert.Equal(new[] { 3, 4 }, late.Select(e => ((Ev)e.Item).Value));
        }

        [Fact]
        public void ChainedWindows_DownstreamWindowSeesUpstreamResultsBeforeTheWatermark()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("ChainedEventTime")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .Merge<Ev>(m => m
                    .Branch("x", b => b
                        .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                        .Map(r => new Ev("x", r.WindowStart, r.Items.Count)))
                    .Branch("y", b => b
                        .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                        .Map(r => new Ev("y", r.WindowStart, r.Items.Count))))
                .TumblingWindow(e => "all", e => e.Time, TimeSpan.FromSeconds(20))
                .Sink(results.Add)
                .Build();
            stream.Start();

            stream.Emit(At(1, 1));
            stream.Emit(At(11, 2));   // W = 11: branch windows [0,10) fire
            stream.Emit(At(25, 3));   // W = 25: branch windows [10,20) fire, then [0,20) downstream

            var window = Assert.Single(results);
            Assert.Equal(new[] { "x", "y", "x", "y" }, window.Items.Select(e => e.Key));
        }

        [Fact]
        public void RuntimeBranch_RunsInEventTime_AndStopsAfterRemoval()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("RuntimeEventTime")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .AddBranch("base", b => b.Sink(_ => { }))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.AddBranch<Ev>("windows", b => b
                .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                .Sink(results.Add));

            stream.Emit(At(1, 1));
            stream.Emit(At(11, 2));   // fires synchronously, so the branch is in event time
            Assert.Equal(new[] { 1 }, Values(Assert.Single(results)));

            Assert.True(stream.RemoveBranch("windows"));
            stream.Emit(At(30, 3));   // [10,20) would fire if the removed window still got watermarks

            Assert.Single(results);
        }

        [Fact]
        public void StreamWithoutWatermarks_KeepsProcessingTimeWindows()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("ProcessingTime")
                .Stream()
                .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                .Sink(r => { lock (results) results.Add(r); })
                .Build();
            stream.Start();

            stream.Emit(At(1, 1));   // the window ended in 2020: the wall-clock timer closes it

            Assert.True(SpinWait.SpinUntil(() => { lock (results) return results.Count == 1; }, TimeSpan.FromSeconds(5)));
        }
    }
}
