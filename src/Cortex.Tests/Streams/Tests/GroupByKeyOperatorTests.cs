using Cortex.States;
using Cortex.Streams.Operators;

namespace Cortex.Streams.Tests
{
    public class GroupByKeyOperatorTests
    {
        [Fact]
        public void Process_ShouldGroupDataByKey()
        {
            // Arrange
            var stateStore = new InMemoryStateStore<string, List<int>>("GroupByTestStore");
            var groupByOperator = new GroupByKeyOperator<int, string>(
                x => x.ToString(),
                stateStore
            );

            // Act
            groupByOperator.Process(1);
            groupByOperator.Process(2);
            groupByOperator.Process(1);

            // Assert
            var group1 = stateStore.Get("1");
            var group2 = stateStore.Get("2");

            Assert.Equal(new[] { 1, 1 }, group1);
            Assert.Equal(new[] { 2 }, group2);
        }
    }
}
