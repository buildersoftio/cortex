using Cortex.Streams;
using Cortex.Streams.ErrorHandling;
using Cortex.Telemetry;
using Moq;
using System.Collections.Concurrent;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Tests for adding and removing branches on a running stream (issue #155).
    /// </summary>
    public class DynamicBranchingTests
    {
        private static IStream<int> BuildStreamWithBranch(string streamName, List<int> staticBranch, List<int> main)
        {
            return StreamBuilder<int>
                .CreateNewStream(streamName)
                .Stream()
                .AddBranch("static", b => b.Sink(x => staticBranch.Add(x)))
                .Sink(x => main.Add(x))
                .Build();
        }

        [Fact]
        public void AddBranch_AtRuntime_ReceivesOnlyItemsEmittedAfterwards()
        {
            var staticBranch = new List<int>();
            var main = new List<int>();
            var dynamicBranch = new List<string>();
            var stream = BuildStreamWithBranch("RuntimeAdd", staticBranch, main);
            stream.Start();

            stream.Emit(1);
            stream.AddBranch<int>("dynamic", b => b
                .Map(x => $"v{x}")
                .Sink(x => dynamicBranch.Add(x)));
            stream.Emit(2);
            stream.Emit(3);

            Assert.Equal(new[] { "v2", "v3" }, dynamicBranch);
            Assert.Equal(new[] { 1, 2, 3 }, staticBranch);
            Assert.Equal(new[] { 1, 2, 3 }, main);
        }

        [Fact]
        public void RemoveBranch_StopsDeliveryToThatBranchOnly()
        {
            var staticBranch = new List<int>();
            var main = new List<int>();
            var dynamicBranch = new List<int>();
            var stream = BuildStreamWithBranch("RuntimeRemove", staticBranch, main);
            stream.Start();
            stream.AddBranch<int>("dynamic", b => b.Sink(x => dynamicBranch.Add(x)));

            stream.Emit(1);
            var removedDynamic = stream.RemoveBranch("dynamic");
            var removedStatic = stream.RemoveBranch("static");
            stream.Emit(2);

            Assert.True(removedDynamic);
            Assert.True(removedStatic);
            Assert.Equal(new[] { 1 }, dynamicBranch);
            Assert.Equal(new[] { 1 }, staticBranch);
            Assert.Equal(new[] { 1, 2 }, main);
            Assert.Empty(stream.GetBranches());
        }

        [Fact]
        public void AddConditionalBranch_AtRuntime_ReceivesOnlyMatchingItems()
        {
            var evens = new List<int>();
            var main = new List<int>();
            var stream = BuildStreamWithBranch("RuntimeConditional", new List<int>(), main);
            stream.Start();

            stream.AddBranch<int>("evens", x => x % 2 == 0, b => b.Sink(x => evens.Add(x)));
            stream.Emit(1);
            stream.Emit(2);
            stream.Emit(4);

            Assert.Equal(new[] { 2, 4 }, evens);
            Assert.Equal(new[] { 1, 2, 4 }, main);
            Assert.Throws<ArgumentNullException>(() =>
                stream.AddBranch<int>("nullCondition", (Func<int, bool>)null, b => b.Sink(_ => { })));
            Assert.Equal(new[] { "evens", "static" }, stream.GetBranches().Keys.OrderBy(k => k));
        }

        [Fact]
        public void RemoveBranch_UnknownOrEmptyName_ReturnsFalse()
        {
            var stream = BuildStreamWithBranch("RemoveUnknown", new List<int>(), new List<int>());

            Assert.False(stream.RemoveBranch("missing"));
            Assert.False(stream.RemoveBranch(""));
            Assert.False(stream.RemoveBranch(null));
            Assert.Single(stream.GetBranches());
        }

        [Fact]
        public void AddBranch_DuplicateName_ThrowsAndKeepsExistingBranch()
        {
            var staticBranch = new List<int>();
            var duplicate = new List<int>();
            var stream = BuildStreamWithBranch("DuplicateName", staticBranch, new List<int>());
            stream.Start();

            var ex = Assert.Throws<ArgumentException>(() =>
                stream.AddBranch<int>("static", b => b.Sink(x => duplicate.Add(x))));
            stream.Emit(1);

            Assert.Contains("static", ex.Message);
            Assert.Equal(new[] { 1 }, staticBranch);
            Assert.Empty(duplicate);
        }

        [Fact]
        public void AddBranch_InvalidArguments_Throw()
        {
            var stream = BuildStreamWithBranch("InvalidArgs", new List<int>(), new List<int>());

            Assert.Throws<ArgumentException>(() => stream.AddBranch<int>("", b => b.Sink(_ => { })));
            Assert.Throws<ArgumentException>(() => stream.AddBranch<int>(null, b => b.Sink(_ => { })));
            Assert.Throws<ArgumentNullException>(() => stream.AddBranch<int>("x", null));
            Assert.Throws<InvalidOperationException>(() => stream.AddBranch<int>("empty", b => { }));
            Assert.Single(stream.GetBranches());
        }

        [Fact]
        public void AddBranch_WithoutBranchPointOfThatType_Throws()
        {
            var noBranches = StreamBuilder<int>
                .CreateNewStream("NoBranchPoint")
                .Stream()
                .Sink(_ => { })
                .Build();
            var intBranches = BuildStreamWithBranch("WrongType", new List<int>(), new List<int>());

            var none = Assert.Throws<InvalidOperationException>(() =>
                noBranches.AddBranch<int>("x", b => b.Sink(_ => { })));
            var wrongType = Assert.Throws<InvalidOperationException>(() =>
                intBranches.AddBranch<string>("x", b => b.Sink(_ => { })));

            Assert.Contains("no branch point", none.Message);
            Assert.Contains("String", wrongType.Message);
        }

        [Fact]
        public void AddBranch_WithSeveralBranchPointsOfThatType_Throws()
        {
            var stream = StreamBuilder<int>
                .CreateNewStream("SeveralBranchPoints")
                .Stream()
                .AddBranch("first", b => b.Sink(_ => { }))
                .Map(x => x + 1)
                .AddBranch("second", b => b.Sink(_ => { }))
                .Sink(_ => { })
                .Build();

            var ex = Assert.Throws<InvalidOperationException>(() =>
                stream.AddBranch<int>("third", b => b.Sink(_ => { })));

            Assert.Contains("2 branch points", ex.Message);
            Assert.Equal(2, stream.GetBranches().Count);
        }

        [Fact]
        public void AddBranch_AtRuntime_UsesStreamErrorHandling()
        {
            var dynamicBranch = new List<int>();
            var main = new List<int>();
            var stream = StreamBuilder<int>
                .CreateNewStream("RuntimeErrorHandling")
                .WithErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip })
                .Stream()
                .AddBranch("static", b => b.Sink(_ => { }))
                .Sink(x => main.Add(x))
                .Build();
            stream.Start();

            stream.AddBranch<int>("dynamic", b => b
                .Map(x =>
                {
                    if (x == 2) throw new InvalidOperationException("boom");
                    return x * 10;
                })
                .Sink(x => dynamicBranch.Add(x)));
            stream.Emit(1);
            stream.Emit(2);
            stream.Emit(3);

            Assert.Equal(new[] { 10, 30 }, dynamicBranch);
            Assert.Equal(new[] { 1, 2, 3 }, main);
            Assert.Equal(StreamStatuses.RUNNING, stream.GetStatus());
        }

        [Fact]
        public void AddBranch_AtRuntime_UsesStreamTelemetry()
        {
            var counters = new ConcurrentDictionary<string, double>();
            var metrics = new Mock<IMetricsProvider>();
            metrics.Setup(m => m.CreateCounter(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string name, string description) =>
                {
                    var counter = new Mock<ICounter>();
                    counter.Setup(c => c.Increment(It.IsAny<double>()))
                        .Callback((double value) => counters.AddOrUpdate(name, value, (_, old) => old + value));
                    return counter.Object;
                });
            metrics.Setup(m => m.CreateHistogram(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(Mock.Of<IHistogram>());
            var tracer = new Mock<ITracer>();
            tracer.Setup(t => t.StartSpan(It.IsAny<string>())).Returns(() => Mock.Of<ISpan>());
            var tracing = new Mock<ITracingProvider>();
            tracing.Setup(t => t.GetTracer(It.IsAny<string>())).Returns(tracer.Object);
            var provider = new Mock<ITelemetryProvider>();
            provider.Setup(p => p.GetMetricsProvider()).Returns(metrics.Object);
            provider.Setup(p => p.GetTracingProvider()).Returns(tracing.Object);

            var stream = StreamBuilder<int>
                .CreateNewStream("RuntimeTelemetry")
                .WithTelemetry(provider.Object)
                .Stream()
                .AddBranch("static", b => b.Sink(_ => { }))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.AddBranch<int>("dynamic", b => b.Map(x => x.ToString()).Sink(_ => { }));
            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(2, counters["branch_operator_processed_dynamic_Int32"]);
            Assert.Equal(2, counters["map_operator_processed_Int32_to_String"]);
        }

        [Fact]
        public void GetBranches_ReflectsRuntimeAddAndRemove()
        {
            var stream = BuildStreamWithBranch("GetBranchesRuntime", new List<int>(), new List<int>());

            stream.AddBranch<int>("dynamic", b => b.Sink(_ => { }));
            Assert.Equal(new[] { "dynamic", "static" }, stream.GetBranches().Keys.OrderBy(k => k));

            stream.RemoveBranch("static");
            Assert.Equal(new[] { "dynamic" }, stream.GetBranches().Keys);
        }

        [Fact]
        public void GetBranches_IncludesBranchesDeclaredBeforeTypeChangingOperator()
        {
            var stream = StreamBuilder<int>
                .CreateNewStream("BranchBeforeMap")
                .Stream()
                .AddBranch("raw", b => b.Sink(_ => { }))
                .Map(x => x.ToString())
                .Sink(_ => { })
                .Build();

            Assert.Equal(new[] { "raw" }, stream.GetBranches().Keys);
        }

        [Fact]
        public void AddBranch_FromInsideRunningBranch_TakesEffectForLaterItems()
        {
            var dynamicBranch = new List<int>();
            IStream<int> stream = null;
            stream = StreamBuilder<int>
                .CreateNewStream("ReentrantAdd")
                .Stream()
                .AddBranch("trigger", b => b.Sink(x =>
                {
                    if (x == 1)
                        stream.AddBranch<int>("late", lb => lb.Sink(y => dynamicBranch.Add(y)));
                }))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(new[] { 2 }, dynamicBranch);
        }

        [Fact]
        public void RemoveBranch_FromInsideRunningBranch_TakesEffectForLaterItems()
        {
            var second = new List<int>();
            IStream<int> stream = null;
            stream = StreamBuilder<int>
                .CreateNewStream("ReentrantRemove")
                .Stream()
                .AddBranch("first", b => b.Sink(x =>
                {
                    if (x == 1)
                        stream.RemoveBranch("second");
                }))
                .AddBranch("second", b => b.Sink(x => second.Add(x)))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            // Item 1 had already read the branch snapshot, so it still reaches "second"
            Assert.Equal(new[] { 1 }, second);
        }

        [Fact]
        public void AddBranch_AtRuntime_AttachesToFanOutBranchPoint()
        {
            var database = new List<int>();
            var dynamicBranch = new List<int>();
            var stream = StreamBuilder<int>
                .CreateNewStream("FanOutRuntimeAdd")
                .Stream()
                .FanOut(fanOut => fanOut.To("database", x => database.Add(x)))
                .Build();
            stream.Start();

            stream.AddBranch<int>("dynamic", b => b.Sink(x => dynamicBranch.Add(x)));
            stream.Emit(7);

            Assert.Equal(new[] { 7 }, database);
            Assert.Equal(new[] { 7 }, dynamicBranch);
            Assert.Equal(new[] { "database", "dynamic" }, stream.GetBranches().Keys.OrderBy(k => k));
        }

        [Fact]
        public void AddBranch_AtRuntime_FindsBranchPointAfterOperatorWithoutNextOperatorList()
        {
            var dynamicBranch = new List<int>();
            var stream = StreamBuilder<int>
                .CreateNewStream("BranchPointAfterGroupBy")
                .Stream()
                .GroupBySilently(x => x % 2)
                .AddBranch("grouped", b => b.Sink(_ => { }))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.AddBranch<int>("dynamic", b => b.Sink(x => dynamicBranch.Add(x)));
            stream.Emit(3);

            Assert.Equal(new[] { 3 }, dynamicBranch);
            Assert.Equal(new[] { "dynamic", "grouped" }, stream.GetBranches().Keys.OrderBy(k => k));
        }

        [Fact]
        public async Task RemoveBranch_DisposesWindowOperatorsOfTheRemovedBranch()
        {
            var windows = new ConcurrentQueue<string>();
            var stream = BuildStreamWithBranch("RemoveDisposesWindow", new List<int>(), new List<int>());
            stream.Start();
            stream.AddBranch<int>("windowed", b => b
                .TumblingWindow(x => "all", x => DateTime.UtcNow, TimeSpan.FromMilliseconds(100))
                .Map(w => w.Key)
                .Sink(k => windows.Enqueue(k)));

            stream.Emit(1);
            Assert.True(stream.RemoveBranch("windowed"));
            await Task.Delay(1000);

            // The window timer stopped with the branch, so the open window is never emitted
            Assert.Empty(windows);
        }

        [Fact]
        public async Task ConcurrentEmitAndBranchChanges_DoNotThrowOrLoseItems()
        {
            var mainCount = 0;
            var staticCount = 0;
            var mutations = 0;
            var emittingDone = 0;
            var stream = StreamBuilder<int>
                .CreateNewStream("ConcurrentBranchChanges")
                .Stream()
                .AddBranch("static", b => b.Sink(_ => Interlocked.Increment(ref staticCount)))
                .Sink(_ => Interlocked.Increment(ref mainCount))
                .Build();
            stream.Start();

            // The mutator starts first and runs until every emitter has finished, so they overlap
            var mutatorStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var mutator = Task.Run(() =>
            {
                while (Volatile.Read(ref emittingDone) == 0)
                {
                    var name = $"dynamic-{mutations}";
                    stream.AddBranch<int>(name, b => b.Sink(_ => { }));
                    mutatorStarted.TrySetResult(true);
                    Assert.True(stream.RemoveBranch(name));
                    mutations++;
                }
            });
            await Task.WhenAny(mutatorStarted.Task, mutator);

            var emitters = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
            {
                for (var i = 0; i < 250; i++)
                    await stream.EmitAsync(i);
            })).ToArray();
            await Task.WhenAll(emitters);
            Volatile.Write(ref emittingDone, 1);
            await mutator;

            Assert.Equal(1000, mainCount);
            Assert.Equal(1000, staticCount);
            Assert.True(mutations > 0);
            Assert.Equal(new[] { "static" }, stream.GetBranches().Keys);
        }
    }
}
