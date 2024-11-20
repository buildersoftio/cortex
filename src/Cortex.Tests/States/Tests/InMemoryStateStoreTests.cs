namespace Cortex.States.Tests
{
    public class InMemoryStateStoreTests
    {
        [Fact]
        public void PutAndGet_ShouldStoreAndRetrieveValue()
        {
            // Arrange
            var store = new InMemoryStateStore<string, int>("TestStore");

            // Act
            store.Put("key1", 42);
            var result = store.Get("key1");

            // Assert
            Assert.Equal(42, result);
        }

        [Fact]
        public void Remove_ShouldDeleteValue()
        {
            // Arrange
            var store = new InMemoryStateStore<string, int?>("TestStore");
            store.Put("key1", 42);

            // Act
            store.Remove("key1");
            var result = store.Get("key1");

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public void ContainsKey_ShouldReturnCorrectly()
        {
            // Arrange
            var store = new InMemoryStateStore<string, int>("TestStore");
            store.Put("key1", 42);

            // Act & Assert
            Assert.True(store.ContainsKey("key1"));
            Assert.False(store.ContainsKey("key2"));
        }
    }
}
