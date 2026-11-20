using Cortex.Streams;
using Cortex.Streams.ErrorHandling;
using Cortex.States;
using Cortex.Telemetry;
using Moq;
using System.Collections.Concurrent;

namespace Cortex.Tests.Streams.Tests
{
    /// <summary>
    /// Tests for conditional branches and merging branch outputs (issue #155).
    /// </summary>
    public class BranchMergeTests
    {
        [Fact]
        public void ConditionalBranch_ReceivesOnlyMatchingItems_MainPathReceivesAll()
        {
            var large = new List<int>();
            var small = new List<int>();
            var main = new List<int>();

            var stream = StreamBuilder<int>
                .CreateNewStream("ConditionalBranches")
                .Stream()
                .AddBranch("large", x => x >= 10, b => b.Sink(x => large.Add(x)))
                .AddBranch("small", x => x < 10, b => b
                    .Map(x => x * 2)
                    .Sink(x => small.Add(x)))
                .Sink(x => main.Add(x))
                .Build();
            stream.Start();

            stream.Emit(5);
            stream.Emit(15);
            stream.Emit(7);

            Assert.Equal(new[] { 15 }, large);
            Assert.Equal(new[] { 10, 14 }, small);
            Assert.Equal(new[] { 5, 15, 7 }, main);
            Assert.Equal(new[] { "large", "small" }, stream.GetBranches().Keys.OrderBy(k => k));
        }

        [Fact]
        public void ConditionalBranch_NullConditionOrEmptyBranch_Throws()
        {
            var builder = StreamBuilder<int>
                .CreateNewStream("ConditionalInvalid")
                .Stream();

            Assert.Throws<ArgumentNullException>(() =>
                builder.AddBranch("x", (Func<int, bool>)null, b => b.Sink(_ => { })));
            Assert.Throws<InvalidOperationException>(() =>
                builder.AddBranch("empty", x => true, b => { }));
        }

        [Fact]
        public void ConditionalBranch_ThrowingCondition_FollowsStreamErrorHandling()
        {
            var branch = new List<int>();
            var main = new List<int>();

            var stream = StreamBuilder<int>
                .CreateNewStream("ConditionalErrorHandling")
                .WithErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip })
                .Stream()
                .AddBranch("guarded", x =>
                {
                    if (x == 2) throw new InvalidOperationException("bad condition");
                    return true;
                }, b => b.Sink(x => branch.Add(x)))
                .Sink(x => main.Add(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);
            stream.Emit(3);

            Assert.Equal(new[] { 1, 3 }, branch);
            Assert.Equal(new[] { 1, 2, 3 }, main);
        }

        [Fact]
        public void ConditionalBranch_ThrowingConditionWithoutErrorHandling_Propagates()
        {
            var stream = StreamBuilder<int>
                .CreateNewStream("ConditionalRethrow")
                .Stream()
                .AddBranch("guarded", x => throw new InvalidOperationException("bad condition"), b => b.Sink(_ => { }))
                .Sink(_ => { })
                .Build();
            stream.Start();

            Assert.Throws<InvalidOperationException>(() => stream.Emit(1));
        }

        [Fact]
        public void Merge_CombinesBranchOutputsIntoOneStream_InBranchOrderPerItem()
        {
            var merged = new List<string>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeOrder")
                .Stream()
                .Merge<string>(m => m
                    .Branch("a", b => b.Map(x => $"a{x}"))
                    .Branch("b", b => b.Map(x => $"b{x}")))
                .Sink(x => merged.Add(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(new[] { "a1", "b1", "a2", "b2" }, merged);
        }

        [Fact]
        public void Merge_ConditionalAndPassThroughBranches()
        {
            var merged = new List<int>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeConditional")
                .Stream()
                .Merge<int>(m => m
                    .Branch("all", b => b)
                    .Branch("evensTimesTen", x => x % 2 == 0, b => b.Map(x => x * 10))
                    .Branch("oddsOnly", x => x % 2 != 0, b => b))
                .Sink(x => merged.Add(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(new[] { 1, 1, 2, 20 }, merged);
        }

        [Fact]
        public void Merge_OperatorsChainedAfterMergeReceiveMergedItems()
        {
            var result = new List<string>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeThenOperators")
                .Stream()
                .Map(x => x + 1)
                .Merge<string>(m => m
                    .Branch("a", b => b.Map(x => $"a{x}"))
                    .Branch("b", b => b.Filter(x => x > 2).Map(x => $"b{x}")))
                .Filter(s => s != "a2")
                .Map(s => s.ToUpperInvariant())
                .Sink(x => result.Add(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(new[] { "A3", "B3" }, result);
        }

        [Fact]
        public void Merge_BranchesAreListedAndRemovable()
        {
            var merged = new List<string>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeRemove")
                .Stream()
                .Merge<string>(m => m
                    .Branch("a", b => b.Map(x => $"a{x}"))
                    .Branch("b", b => b.Map(x => $"b{x}")))
                .Sink(x => merged.Add(x))
                .Build();
            stream.Start();

            Assert.Equal(new[] { "a", "b" }, stream.GetBranches().Keys.OrderBy(k => k));

            stream.Emit(1);
            Assert.True(stream.RemoveBranch("b"));
            stream.Emit(2);

            Assert.Equal(new[] { "a1", "b1", "a2" }, merged);
            Assert.Equal(new[] { "a" }, stream.GetBranches().Keys);
        }

        [Fact]
        public void Merge_ErrorHandlingReachesOperatorsAfterMerge()
        {
            var merged = new List<int>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeErrorHandling")
                .WithErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip })
                .Stream()
                .Merge<int>(m => m
                    .Branch("a", b => b)
                    .Branch("b", b => b.Map(x => x * 100)))
                .Map(x =>
                {
                    if (x == 200) throw new InvalidOperationException("boom");
                    return x;
                })
                .Sink(x => merged.Add(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(new[] { 1, 100, 2 }, merged);
        }

        [Fact]
        public void Merge_TelemetryCountsEachMergedItemOnce()
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
                .CreateNewStream("MergeTelemetry")
                .WithTelemetry(provider.Object)
                .Stream()
                .Merge<string>(m => m
                    .Branch("all", b => b.Map(x => $"all{x}"))
                    .Branch("evens", x => x % 2 == 0, b => b.Map(x => $"even{x}")))
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            Assert.Equal(3, counters["merge_operator_processed_String"]);
            Assert.Equal(3, counters["sink_operator_processed_String"]);
        }

        [Fact]
        public void Merge_StateStoresInBranchesAndAfterMergeAreDiscoverable()
        {
            var stream = StreamBuilder<int>
                .CreateNewStream("MergeStateStores")
                .Stream()
                .Merge<int>(m => m
                    .Branch("grouped", b => b.GroupBySilently(x => x % 2, "branch-groups"))
                    .Branch("raw", b => b))
                .AggregateSilently(x => "total", (int sum, int x) => sum + x, "after-merge-total")
                .Sink(_ => { })
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);

            var branchStore = stream.GetStateStoreByName<InMemoryStateStore<int, List<int>>>("branch-groups");
            var totalStore = stream.GetStateStoreByName<InMemoryStateStore<string, int>>("after-merge-total");
            Assert.NotNull(branchStore);
            Assert.NotNull(totalStore);
            Assert.Equal(6, totalStore.Get("total"));
        }

        [Fact]
        public void Merge_InvalidConfiguration_Throws()
        {
            var builder = StreamBuilder<int>
                .CreateNewStream("MergeInvalid")
                .Stream();

            Assert.Throws<ArgumentNullException>(() => builder.Merge<int>(null));
            Assert.Throws<InvalidOperationException>(() => builder.Merge<int>(m => { }));
            Assert.Throws<ArgumentException>(() => builder.Merge<int>(m => m
                .Branch("same", b => b)
                .Branch("same", b => b)));
            Assert.Throws<ArgumentException>(() => builder.Merge<int>(m => m.Branch("", b => b)));
            Assert.Throws<ArgumentNullException>(() => builder.Merge<int>(m => m.Branch("a", null)));
            Assert.Throws<ArgumentNullException>(() => builder.Merge<int>(m => m.Branch("a", (Func<int, bool>)null, b => b)));
            Assert.Throws<ArgumentException>(() => builder.Merge<string>(m => m
                .Branch("unrelated", b => new BranchStreamBuilder<int, string>("other"))));
        }

        [Fact]
        public void Merge_IsNotTargetedByRuntimeAddBranch()
        {
            var onlyMerge = StreamBuilder<int>
                .CreateNewStream("MergeOnly")
                .Stream()
                .Merge<int>(m => m.Branch("a", b => b))
                .Sink(_ => { })
                .Build();

            var dynamicBranch = new List<int>();
            var merged = new List<int>();
            var withBranchPoint = StreamBuilder<int>
                .CreateNewStream("MergeAndBranchPoint")
                .Stream()
                .AddBranch("audit", b => b.Sink(_ => { }))
                .Merge<int>(m => m.Branch("a", b => b))
                .Sink(x => merged.Add(x))
                .Build();
            withBranchPoint.Start();

            Assert.Throws<InvalidOperationException>(() =>
                onlyMerge.AddBranch<int>("x", b => b.Sink(_ => { })));
            withBranchPoint.AddBranch<int>("dynamic", b => b.Sink(x => dynamicBranch.Add(x)));
            withBranchPoint.Emit(5);

            Assert.Equal(new[] { 5 }, dynamicBranch);
            Assert.Equal(new[] { 5 }, merged);
        }

        [Fact]
        public void Merge_FailedConfigurationLeavesPipelineUnchanged()
        {
            var main = new List<int>();
            var builder = StreamBuilder<int>
                .CreateNewStream("MergeFailedThenUsed")
                .Stream();

            Assert.Throws<ArgumentException>(() => builder.Merge<int>(m => m
                .Branch("a", b => b.Map(x => x * 100))
                .Branch("a", b => b)));
            var stream = builder.Sink(x => main.Add(x)).Build();
            stream.Start();
            stream.Emit(1);

            Assert.Equal(new[] { 1 }, main);
            Assert.Empty(stream.GetBranches());
        }

        [Fact]
        public void RuntimeAddBranch_NameUsedByMergeBranch_Throws()
        {
            var stream = StreamBuilder<int>
                .CreateNewStream("RuntimeNameClash")
                .Stream()
                .AddBranch("audit", b => b.Sink(_ => { }))
                .Merge<int>(m => m.Branch("a", b => b))
                .Sink(_ => { })
                .Build();

            Assert.Throws<ArgumentException>(() => stream.AddBranch<int>("a", b => b.Sink(_ => { })));
            Assert.Equal(new[] { "a", "audit" }, stream.GetBranches().Keys.OrderBy(k => k));
        }

        [Fact]
        public void RemoveMergeBranch_KeepsOperatorsAfterTheMergeRunning()
        {
            var windows = new ConcurrentQueue<int>();
            var stream = StreamBuilder<int>
                .CreateNewStream("RemoveMergeBranchKeepsWindow")
                .Stream()
                .Merge<int>(m => m
                    .Branch("a", b => b)
                    .Branch("b", b => b.Map(x => x * 10)))
                .TumblingWindow(x => "all", x => DateTime.UtcNow, TimeSpan.FromMilliseconds(100))
                .Sink(w => windows.Enqueue(w.Items.Count))
                .Build();
            stream.Start();

            Assert.True(stream.RemoveBranch("b"));
            stream.Emit(1);

            // The window after the merge is shared by the remaining branch and must keep emitting
            Assert.True(SpinWait.SpinUntil(() => !windows.IsEmpty, TimeSpan.FromSeconds(5)));
        }

        [Fact]
        public async Task EndToEnd_MergeWithRuntimeBranchChanges()
        {
            var merged = new ConcurrentQueue<string>();
            var large = new ConcurrentQueue<int>();

            var stream = StreamBuilder<int>
                .CreateNewStream("MergeEndToEnd")
                .Stream()
                .AddBranch("audit", b => b.Sink(_ => { }))
                .Merge<string>(m => m
                    .Branch("plain", b => b.Map(x => $"p{x}"))
                    .Branch("evens", x => x % 2 == 0, b => b.Map(x => $"e{x}")))
                .Sink(x => merged.Enqueue(x))
                .Build();
            stream.Start();

            stream.Emit(1);
            stream.Emit(2);
            stream.AddBranch<int>("large", x => x >= 100, b => b.Sink(x => large.Enqueue(x)));
            Assert.True(stream.RemoveBranch("evens"));
            stream.Emit(4);
            stream.Emit(100);

            Assert.Equal(new[] { "p1", "p2", "e2", "p4", "p100" }, merged);
            Assert.Equal(new[] { 100 }, large);

            // The mutator starts first and runs until every emitter has finished, so they overlap
            var emittingDone = 0;
            var mutations = 0;
            var mutatorStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var mutator = Task.Run(() =>
            {
                while (Volatile.Read(ref emittingDone) == 0)
                {
                    var name = $"temp-{mutations}";
                    stream.AddBranch<int>(name, x => x % 3 == 0, b => b.Sink(_ => { }));
                    mutatorStarted.TrySetResult(true);
                    Assert.True(stream.RemoveBranch(name));
                    mutations++;
                }
            });
            await Task.WhenAny(mutatorStarted.Task, mutator);

            var emitters = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
            {
                for (var i = 0; i < 100; i++)
                    await stream.EmitAsync(i);
            })).ToArray();
            await Task.WhenAll(emitters);
            Volatile.Write(ref emittingDone, 1);
            await mutator;

            Assert.Equal(5 + 400, merged.Count);
            Assert.True(mutations > 0);
            Assert.Equal(new[] { "audit", "large", "plain" }, stream.GetBranches().Keys.OrderBy(k => k));
        }
    }
}
