using Cortex.States;
using Cortex.Streams;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Saving and restoring the watermark through a state store (issue #197).
    /// </summary>
    public class WatermarkPersistenceTests
    {
        private static readonly DateTime T0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private sealed record Ev(string Key, DateTime Time, int Value);

        private static Ev At(int seconds, int value) => new Ev("a", T0.AddSeconds(seconds), value);

        private static IStream<Ev> Build(WatermarkStrategy strategy, List<WindowResult<string, Ev>> results)
        {
            var stream = StreamBuilder<Ev>
                .CreateNewStream("PersistedWatermarks")
                .Stream()
                .WithWatermarks(e => e.Time, strategy)
                .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromSeconds(10))
                .Sink(results.Add)
                .Build();
            stream.Start();
            return stream;
        }

        [Fact]
        public void RebuiltStream_ResumesAtTheSavedWatermark()
        {
            var store = new InMemoryStateStore<string, long>("watermarks");
            var late = new List<LateEvent>();
            var strategy = WatermarkStrategy.ForMonotonousTimestamps().WithStateStore(store).OnLateEvent(late.Add);

            var first = Build(strategy, new List<WindowResult<string, Ev>>());
            first.Emit(At(1, 1));
            first.Emit(At(25, 2));
            first.Stop();

            var results = new List<WindowResult<string, Ev>>();
            var second = Build(strategy, results);
            second.Emit(At(5, 3));    // older than the restored watermark 25: late
            second.Emit(At(26, 4));
            second.Emit(At(31, 5));   // W = 31: [20,30) fires with what this run saw

            Assert.Equal(3, ((Ev)Assert.Single(late).Item).Value);
            Assert.Equal(new[] { 4 }, Assert.Single(results).Items.Select(e => e.Value));
        }

        [Fact]
        public void Saves_AtMostOncePerSecond_AndOnStop()
        {
            var now = new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc);
            var store = new InMemoryStateStore<string, long>("watermarks");
            var strategy = WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(1))
                .WithStateStore(store)
                .WithClock(() => now);
            var stream = Build(strategy, new List<WindowResult<string, Ev>>());

            long Saved() => store.Get(WatermarkStrategy.WatermarkStateKey);

            stream.Emit(At(10, 1));                              // first advance: saved
            Assert.Equal(T0.AddSeconds(9).Ticks, Saved());
            Assert.Equal(T0.AddSeconds(10).Ticks, store.Get(WatermarkStrategy.MaxTimestampStateKey));

            stream.Emit(At(11, 2));                              // same second: not saved
            Assert.Equal(T0.AddSeconds(9).Ticks, Saved());

            now = now.AddSeconds(1);
            stream.Emit(At(12, 3));                              // a second later: saved
            Assert.Equal(T0.AddSeconds(11).Ticks, Saved());

            stream.Emit(At(13, 4));
            stream.Stop();                                       // flushed on stop
            Assert.Equal(T0.AddSeconds(12).Ticks, Saved());
        }

        [Fact]
        public void Store_IsListedWithTheStreamsStateStores()
        {
            var store = new InMemoryStateStore<string, long>("watermarks");
            var stream = Build(WatermarkStrategy.ForMonotonousTimestamps().WithStateStore(store), new List<WindowResult<string, Ev>>());

            Assert.Same(store, stream.GetStateStoreByName<InMemoryStateStore<string, long>>("watermarks"));
        }
    }
}
