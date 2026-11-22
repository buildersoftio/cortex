using Cortex.Streams;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Event-time stream-stream joins (issue #197). Timestamps are in 2020, so wall-clock expiry would drop everything.
    /// </summary>
    public class EventTimeJoinTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Order(string Id, DateTime Time);

        private sealed record Payment(string OrderId, DateTime Time);

        private static Order OrderAt(string id, double seconds) => new Order(id, T0.AddSeconds(seconds));

        private static Payment PaymentAt(string orderId, double seconds) => new Payment(orderId, T0.AddSeconds(seconds));

        private static (IStream<Order> Stream, StreamStreamJoinOperator<Order, Payment, string, string> Join) Build(
            WatermarkStrategy strategy, StreamJoinType joinType, List<string> results, TimeSpan? grace = null)
        {
            var join = new StreamStreamJoinOperator<Order, Payment, string, string>(
                o => o.Id,
                p => p.OrderId,
                o => o.Time,
                p => p.Time,
                (o, p) => $"{o?.Id ?? "-"}:{(p == null ? "-" : ((int)(p.Time - T0).TotalSeconds).ToString())}",
                new StreamJoinConfiguration
                {
                    WindowSize = TimeSpan.FromSeconds(10),
                    JoinType = joinType,
                    GracePeriod = grace ?? TimeSpan.Zero,
                    CleanupInterval = TimeSpan.FromHours(1)
                });

            var stream = StreamBuilder<Order>
                .CreateNewStream("EventTimeJoin")
                .Stream()
                .WithWatermarks(o => o.Time, strategy)
                .JoinStream(join)
                .Sink(r => { lock (results) results.Add(r); })
                .Build();
            stream.Start();
            return (stream, join);
        }

        [Fact]
        public void ReplayedEvents_MatchByEventTimeDistance()
        {
            var results = new List<string>();
            var (stream, join) = Build(WatermarkStrategy.ForMonotonousTimestamps(), StreamJoinType.Inner, results);

            stream.Emit(OrderAt("A", 0));
            join.ProcessRight(PaymentAt("A", 5));    // 5 s apart: a match although both are years old
            join.ProcessRight(PaymentAt("A", 15));   // 15 s apart: outside the 10 s window

            Assert.Equal(new[] { "A:5" }, results);
        }

        [Fact]
        public void NothingExpires_UntilBothSidesHaveAdvanced()
        {
            var results = new List<string>();
            var (stream, join) = Build(WatermarkStrategy.ForMonotonousTimestamps(), StreamJoinType.Left, results);

            stream.Emit(OrderAt("A", 0));
            stream.Emit(OrderAt("B", 30));   // left watermark 30, right side has no events yet
            join.ForceCleanup();
            Assert.Empty(results);

            join.ProcessRight(PaymentAt("C", 25));   // watermark = min(30, 25): A (0 + 10) expires, B does not

            Assert.Equal(new[] { "A:-" }, results);
        }

        [Fact]
        public void IdleRightSide_StopsHoldingTheLeftBack()
        {
            var now = new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc);
            var strategy = WatermarkStrategy.ForMonotonousTimestamps()
                .WithIdleTimeout(TimeSpan.FromSeconds(10))
                .WithClock(() => now);
            var results = new List<string>();
            var (stream, join) = Build(strategy, StreamJoinType.Left, results);

            stream.Emit(OrderAt("A", 0));
            stream.Emit(OrderAt("B", 30));
            join.ForceCleanup();
            Assert.Empty(results);

            now = now.AddSeconds(11);   // no right events for longer than the idle timeout
            join.ForceCleanup();

            Assert.Equal(new[] { "A:-" }, results);
        }

        [Fact]
        public void UnmatchedResults_WaitForWindowPlusGrace()
        {
            var results = new List<string>();
            var (stream, join) = Build(
                WatermarkStrategy.ForMonotonousTimestamps(), StreamJoinType.Outer, results, TimeSpan.FromSeconds(5));

            stream.Emit(OrderAt("A", 0));
            join.ProcessRight(PaymentAt("X", 14));
            stream.Emit(OrderAt("B", 14));   // watermark 14 < 0 + 10 + 5
            Assert.Empty(results);

            join.ProcessRight(PaymentAt("Y", 15));
            stream.Emit(OrderAt("C", 15));   // watermark 15: A expires; X (14 + 15 = 29) not yet

            Assert.Equal(new[] { "A:-" }, results);
        }

        [Fact]
        public void LateElements_AreReported_AndNotBuffered()
        {
            var late = new List<LateEvent>();
            var results = new List<string>();
            var (stream, join) = Build(
                WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add), StreamJoinType.Inner, results);

            stream.Emit(OrderAt("A", 0));
            join.ProcessRight(PaymentAt("Z", 30));
            stream.Emit(OrderAt("B", 30));   // watermark 30: A expired

            stream.Emit(OrderAt("C", 5));        // 5 + 10 <= 30: late
            join.ProcessRight(PaymentAt("A", 3)); // 3 + 10 <= 30: late, and A is gone anyway

            Assert.Equal(new object[] { OrderAt("C", 5), PaymentAt("A", 3) }, late.Select(e => e.Item));
            Assert.All(late, e => Assert.Contains("StreamStreamJoinOperator", e.OperatorName));
            Assert.Equal(1, join.GetLeftBufferCount());    // B only
            Assert.Equal(1, join.GetRightBufferCount());   // Z only
            Assert.Empty(results);
        }
    }
}
