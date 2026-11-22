using Cortex.Streams;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Event-time session windows driven by watermarks (issue #197). Inactivity gap: 10 s.
    /// </summary>
    public class EventTimeSessionWindowTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Ev(string Key, DateTime Time, int Value);

        private static Ev At(double seconds, int value, string key = "a") => new Ev(key, T0.AddSeconds(seconds), value);

        private static int[] Values(WindowResult<string, Ev> window) => window.Items.Select(e => e.Value).ToArray();

        private static IStream<Ev> Sessions(WatermarkStrategy strategy, List<WindowResult<string, Ev>> results)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("EventTimeSessions")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .SessionWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                .Sink(results.Add)
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void OutOfOrderEventInsideTheGap_ExtendsTheSession()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(5)), results);

            stream.Emit(At(3, 1));
            stream.Emit(At(5, 2));
            stream.Emit(At(0, 3));    // earlier than the session start: start moves back to 0
            Assert.Empty(results);

            stream.Emit(At(30, 4));   // W = 25 > 5 + 10

            var session = Assert.Single(results);
            Assert.Equal("a", session.Key);
            Assert.Equal(T0, session.WindowStart);
            Assert.Equal(T0.AddSeconds(15), session.WindowEnd);
            Assert.Equal(new[] { 1, 2, 3 }, Values(session));
        }

        [Fact]
        public void EventBridgingTwoSessions_MergesThem()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(20)), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(18, 2));   // 18 s after the first: a second session
            stream.Emit(At(9, 3));    // within 10 s of both: one session 0..18
            stream.Emit(At(60, 4));   // W = 40

            var session = Assert.Single(results);
            Assert.Equal(T0, session.WindowStart);
            Assert.Equal(T0.AddSeconds(28), session.WindowEnd);
            Assert.Equal(new[] { 1, 2, 3 }, Values(session));
        }

        [Fact]
        public void FarAheadEvent_OpensASecondSession_WithoutClosingTheFirst()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(30)), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(25, 2));   // processing time would close the first session here
            Assert.Empty(results);

            stream.Emit(At(3, 3));    // still joins the first session
            stream.Emit(At(100, 4));  // W = 70: both sessions fire, earliest end first

            Assert.Equal(new[] { new[] { 1, 3 }, new[] { 2 } }, results.Select(Values));
            Assert.Equal(T0.AddSeconds(13), results[0].WindowEnd);
            Assert.Equal(T0.AddSeconds(35), results[1].WindowEnd);
        }

        [Fact]
        public void Session_FiresOnceTheWatermarkPassesLastActivityPlusGap()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.ForMonotonousTimestamps(), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(10, 2, "b"));   // W = 10: an event at exactly 0 + 10 could still join
            Assert.Empty(results);

            stream.Emit(At(10.5, 3, "b")); // W = 10.5

            Assert.Equal(new[] { 1 }, Values(Assert.Single(results)));
        }

        [Fact]
        public void EventPastItsLateness_IsDroppedAndReported()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.ForMonotonousTimestamps().OnLateEvent(late.Add), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(30, 2, "b"));   // W = 30: session a fires
            stream.Emit(At(5, 3));         // 5 + 10 < 30: late

            Assert.Equal(new[] { 1 }, Values(Assert.Single(results)));
            var dropped = Assert.Single(late);
            Assert.Equal(3, ((Ev)dropped.Item).Value);
            Assert.Contains("SessionWindowOperator", dropped.OperatorName);
        }

        [Fact]
        public void SessionsAlreadyInTheStore_AreNotOverwrittenByNewSessions()
        {
            // A durable store can hold sessions from an earlier run under the event-time key format
            var store = new Cortex.States.InMemoryStateStore<string, SessionState<Ev>>("sessions");
            store.Put("session_a_1", new SessionState<Ev>
            {
                Key = "a",
                StartTime = T0,
                LastActivityTime = T0,
                Items = new List<Ev> { At(0, 1) }
            });

            var results = new List<WindowResult<string, Ev>>();
            var stream = StreamBuilder<Ev>
                .CreateNewStream("RestoredSessions")
                .Stream()
                .WithWatermarks(e => e.Time, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromMinutes(5)))
                .SessionWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10), stateStore: store)
                .Sink(results.Add)
                .Build();
            stream.Start();

            stream.Emit(At(50, 2));    // a second session for key a
            stream.Emit(At(500, 3, "b"));

            Assert.Equal(new[] { new[] { 1 }, new[] { 2 } }, results.Select(Values));
        }

        [Fact]
        public void LateEventBridgingTwoFiredSessions_RefiresTheMergedSession()
        {
            var results = new List<WindowResult<string, Ev>>();
            var stream = Sessions(WatermarkStrategy.ForMonotonousTimestamps().WithAllowedLateness(TimeSpan.FromSeconds(30)), results);

            stream.Emit(At(0, 1));
            stream.Emit(At(18, 2));
            stream.Emit(At(40, 3, "b"));   // W = 40: both sessions of a fired, kept until W > their end + 30
            stream.Emit(At(9, 4));         // bridges them

            Assert.Equal(new[] { new[] { 1 }, new[] { 2 }, new[] { 1, 2, 4 } }, results.Select(Values));
            Assert.Equal(T0, results[2].WindowStart);
            Assert.Equal(T0.AddSeconds(28), results[2].WindowEnd);
        }

        [Fact]
        public void AllowedLateness_RefiresTheMergedSession_ThenExpires()
        {
            var late = new List<LateEvent>();
            var results = new List<WindowResult<string, Ev>>();
            var strategy = WatermarkStrategy.ForMonotonousTimestamps()
                .WithAllowedLateness(TimeSpan.FromSeconds(10))
                .OnLateEvent(late.Add);
            var stream = Sessions(strategy, results);

            stream.Emit(At(0, 1));
            stream.Emit(At(15, 2, "b"));   // W = 15: a fires, kept until W > 20
            stream.Emit(At(3, 3));         // allowed late: a fires again with both items
            stream.Emit(At(40, 4, "b"));   // W = 40: a expires, b's first session fires
            stream.Emit(At(4, 5));         // 4 + 10 + 10 < 40: late

            Assert.Equal(new[] { new[] { 1 }, new[] { 1, 3 }, new[] { 2 } }, results.Select(Values));
            Assert.Equal(5, ((Ev)Assert.Single(late).Item).Value);
        }
    }
}
