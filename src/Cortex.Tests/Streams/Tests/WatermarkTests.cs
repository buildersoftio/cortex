using Cortex.Streams;
using Cortex.Streams.Operators;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Tests for watermark generation and propagation (issue #197).
    /// </summary>
    public class WatermarkTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Ev(string Key, DateTime Time, int Value);

        /// <summary>
        /// Pass-through operator that records the items and watermarks it sees in a shared log.
        /// </summary>
        private sealed class Probe : IOperator, IHasNextOperators, IWatermarkAware
        {
            private readonly string _name;
            private readonly List<string> _log;
            private IOperator _nextOperator;

            public Probe(string name, List<string> log)
            {
                _name = name;
                _log = log;
            }

            public List<DateTime> Watermarks { get; } = new List<DateTime>();
            public int EnableCount { get; private set; }
            public EventTimeContext Context { get; private set; }

            public void Process(object input)
            {
                lock (_log) _log.Add($"{_name}:item:{((Ev)input).Value}");
                _nextOperator?.Process(input);
            }

            public void SetNext(IOperator nextOperator) => _nextOperator = nextOperator;

            public IEnumerable<IOperator> GetNextOperators()
            {
                if (_nextOperator != null)
                    yield return _nextOperator;
            }

            public void EnableEventTime(EventTimeContext context)
            {
                EnableCount++;
                Context = context;
            }

            public void OnWatermark(DateTime watermark)
            {
                lock (_log)
                {
                    Watermarks.Add(watermark);
                    _log.Add($"{_name}:wm:{(watermark - T0).TotalSeconds}");
                }
            }
        }

        private static IStream<Ev> BuildWithProbe(WatermarkStrategy strategy, Probe probe)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("Watermarks")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .SetNext(probe)
                .Sink(_ => { })
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void Watermark_TrailsMaxTimestampByBound_AndNeverMovesBack()
        {
            var probe = new Probe("p", new List<string>());
            var stream = BuildWithProbe(WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5)), probe);

            stream.Emit(new Ev("a", T0.AddSeconds(10), 1));
            stream.Emit(new Ev("a", T0.AddSeconds(8), 2));   // out of order: no new watermark
            stream.Emit(new Ev("a", T0.AddSeconds(20), 3));
            stream.Emit(new Ev("a", T0.AddSeconds(12), 4));  // out of order: no new watermark

            Assert.Equal(new[] { T0.AddSeconds(5), T0.AddSeconds(15) }, probe.Watermarks);
        }

        [Fact]
        public void MonotonousTimestamps_WatermarkEqualsNewestTimestamp()
        {
            var probe = new Probe("p", new List<string>());
            var stream = BuildWithProbe(WatermarkStrategy.ForMonotonousTimestamps(), probe);

            stream.Emit(new Ev("a", T0.AddSeconds(3), 1));
            stream.Emit(new Ev("a", T0.AddSeconds(7), 2));

            Assert.Equal(new[] { T0.AddSeconds(3), T0.AddSeconds(7) }, probe.Watermarks);
        }

        [Fact]
        public void Item_ReachesOperatorBeforeTheWatermarkItAdvanced()
        {
            var log = new List<string>();
            var stream = BuildWithProbe(WatermarkStrategy.ForMonotonousTimestamps(), new Probe("p", log));

            stream.Emit(new Ev("a", T0.AddSeconds(4), 1));

            Assert.Equal(new[] { "p:item:1", "p:wm:4" }, log);
        }

        [Fact]
        public void Watermarks_ReachOperatorsInPipelineOrder_OncePerAdvance()
        {
            var log = new List<string>();
            var before = new Probe("before", log);
            var after = new Probe("after", log);

            var stream = StreamBuilder<Ev>
                .CreateNewStream("Order")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .SetNext(before)
                .Merge<Ev>(m => m
                    .Branch("x", b => b)
                    .Branch("y", b => b.Map(e => e with { Value = e.Value + 100 })))
                .SetNext(after)
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(new Ev("a", T0.AddSeconds(1), 1));

            Assert.Equal(
                new[] { "before:item:1", "after:item:1", "after:item:101", "before:wm:1", "after:wm:1" },
                log);
        }

        [Fact]
        public void EventTime_IsEnabledOnceWithTheStrategy()
        {
            var probe = new Probe("p", new List<string>());
            var strategy = WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(1))
                .WithAllowedLateness(TimeSpan.FromSeconds(2));

            BuildWithProbe(strategy, probe);

            Assert.Equal(1, probe.EnableCount);
            Assert.Equal(TimeSpan.FromSeconds(2), probe.Context.AllowedLateness);
            Assert.Empty(probe.Watermarks);
        }

        [Fact]
        public void StreamWithoutWatermarks_NeverSwitchesOperatorsToEventTime()
        {
            var probe = new Probe("p", new List<string>());
            var stream = StreamBuilder<Ev>
                .CreateNewStream("NoWatermarks")
                .Stream()
                .SetNext(probe)
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(new Ev("a", T0.AddSeconds(4), 1));

            Assert.Equal(0, probe.EnableCount);
            Assert.Empty(probe.Watermarks);
        }

        [Fact]
        public void OperatorsUpstreamOfWithWatermarks_StayOnProcessingTime()
        {
            var upstream = new Probe("up", new List<string>());
            var downstream = new Probe("down", new List<string>());

            var stream = StreamBuilder<Ev>
                .CreateNewStream("Upstream")
                .Stream()
                .SetNext(upstream)
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .SetNext(downstream)
                .Sink(_ => { })
                .Build();
            stream.Start();
            stream.Emit(new Ev("a", T0.AddSeconds(4), 1));

            Assert.Equal(0, upstream.EnableCount);
            Assert.Single(downstream.Watermarks);
        }

        [Fact]
        public void WithWatermarksTwiceOnOnePath_ThrowsAtBuild()
        {
            var builder = StreamBuilder<Ev>
                .CreateNewStream("Twice")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .Map(e => e)
                .WithWatermarks(e => e.Time, WatermarkStrategy.ForMonotonousTimestamps())
                .Sink(_ => { });

            Assert.Throws<InvalidOperationException>(() => builder.Build());
        }

        [Fact]
        public void Strategy_ValidatesArguments_AndIsImmutable()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(-1)));
            var strategy = WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5));
            Assert.Throws<ArgumentOutOfRangeException>(() => strategy.WithIdleTimeout(TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(() => strategy.WithAllowedLateness(TimeSpan.FromSeconds(-1)));
            Assert.Throws<ArgumentNullException>(() => strategy.OnLateEvent(null));

            var idle = strategy.WithIdleTimeout(TimeSpan.FromSeconds(1));

            Assert.Null(strategy.IdleTimeout);
            Assert.Equal(TimeSpan.FromSeconds(1), idle.IdleTimeout);
            Assert.Equal(TimeSpan.FromSeconds(5), idle.MaxOutOfOrderness);
        }

        [Fact]
        public void IdleSource_WatermarkAdvancesFromWhereItStopped_AtWallClockSpeed()
        {
            var now = new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc);
            var strategy = WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5))
                .WithIdleTimeout(TimeSpan.FromSeconds(10))
                .WithClock(() => now);
            var watermarks = new WatermarkOperator<Ev>(e => e.Time, strategy);
            var probe = new Probe("p", new List<string>());
            var stream = StreamBuilder<Ev>
                .CreateNewStream("Idle")
                .Stream()
                .SetNext(watermarks)
                .SetNext(probe)
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(new Ev("a", T0.AddSeconds(60), 1));   // watermark 55
            watermarks.CheckIdle();                            // a check notes the activity (the timer does this)
            now = now.AddSeconds(10);
            watermarks.CheckIdle();                            // idle timeout just reached: no change
            Assert.Equal(T0.AddSeconds(55), watermarks.CurrentWatermark);

            now = now.AddSeconds(3);
            watermarks.CheckIdle();                            // idle for 3 s: 55 + 3
            Assert.Equal(T0.AddSeconds(58), watermarks.CurrentWatermark);

            stream.Emit(new Ev("a", T0.AddSeconds(61), 2));   // 61 - 5 = 56 < 58: unchanged, idle clock reset
            now = now.AddSeconds(5);
            watermarks.CheckIdle();
            Assert.Equal(T0.AddSeconds(58), watermarks.CurrentWatermark);

            Assert.Equal(new[] { T0.AddSeconds(55), T0.AddSeconds(58) }, probe.Watermarks);
        }

        [Fact]
        public void IdleClock_DoesNotRunWhileTheStreamIsStopped()
        {
            var now = new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc);
            var strategy = WatermarkStrategy.ForMonotonousTimestamps()
                .WithIdleTimeout(TimeSpan.FromSeconds(10))
                .WithClock(() => now);
            var watermarks = new WatermarkOperator<Ev>(e => e.Time, strategy);
            var stream = StreamBuilder<Ev>
                .CreateNewStream("IdleStopped")
                .Stream()
                .SetNext(watermarks)
                .Sink(_ => { })
                .Build();
            stream.Start();
            stream.Emit(new Ev("a", T0.AddSeconds(60), 1));

            stream.Stop();
            now = now.AddHours(1);
            stream.Start();
            watermarks.CheckIdle();
            Assert.Equal(T0.AddSeconds(60), watermarks.CurrentWatermark);

            now = now.AddSeconds(12);
            watermarks.CheckIdle();
            Assert.Equal(T0.AddSeconds(62), watermarks.CurrentWatermark);
        }

        [Fact]
        public void IdleTimer_AdvancesTheWatermarkWithoutEvents()
        {
            var probe = new Probe("p", new List<string>());
            var strategy = WatermarkStrategy.ForMonotonousTimestamps()
                .WithIdleTimeout(TimeSpan.FromMilliseconds(50));
            var stream = BuildWithProbe(strategy, probe);

            stream.Emit(new Ev("a", T0.AddSeconds(1), 1));

            Assert.True(SpinWait.SpinUntil(() => { lock (probe.Watermarks) return probe.Watermarks.Count >= 2; }, TimeSpan.FromSeconds(5)));
            stream.Stop();
        }

        [Fact]
        public void LateEvents_AreCountedAndPassedToTheHandler_HandlerExceptionsIgnored()
        {
            var received = new List<LateEvent>();
            var context = new EventTimeContext(WatermarkStrategy.ForMonotonousTimestamps()
                .OnLateEvent(e =>
                {
                    received.Add(e);
                    throw new InvalidOperationException("handler failure");
                }));

            context.ReportLateEvent("item", T0, T0.AddSeconds(5), "Op");

            var late = Assert.Single(received);
            Assert.Equal("item", late.Item);
            Assert.Equal(T0, late.Timestamp);
            Assert.Equal(T0.AddSeconds(5), late.Watermark);
            Assert.Equal("Op", late.OperatorName);
            Assert.Equal(1, context.LateEventCount);
        }
    }
}
