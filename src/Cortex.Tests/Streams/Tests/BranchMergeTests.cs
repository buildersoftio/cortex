using Cortex.Streams;
using Cortex.Streams.ErrorHandling;

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
    }
}
