using Cortex.Mediator.Caching;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.Options;

namespace Cortex.Tests.Mediator.Tests
{
    public class DefaultCacheKeyGeneratorTests
    {
        public class TestQuery : IQuery<string>
        {
            public string Input { get; set; } = string.Empty;
            public int Number { get; set; }
        }

        [Cacheable(CacheKeyPrefix = "CustomPrefix")]
        public class CustomPrefixQuery : IQuery<string>
        {
            public string Input { get; set; } = string.Empty;
        }

        public class CacheableQuery : IQuery<string>, ICacheableQuery
        {
            public string Input { get; set; } = string.Empty;
            public string? CacheKey => $"my-custom-key-{Input}";
            public TimeSpan? AbsoluteExpiration => null;
            public TimeSpan? SlidingExpiration => null;
        }

        private readonly DefaultCacheKeyGenerator _generator;

        public DefaultCacheKeyGeneratorTests()
        {
            _generator = new DefaultCacheKeyGenerator(Options.Create(new CachingOptions
            {
                CacheKeyPrefix = "TestPrefix",
                IncludeQueryPropertiesInCacheKey = true
            }));
        }

        [Fact]
        public void GenerateKey_ShouldIncludePrefix()
        {
            // Arrange
            var query = new TestQuery { Input = "test", Number = 42 };

            // Act
            var key = _generator.GenerateKey<TestQuery, string>(query);

            // Assert
            Assert.StartsWith("TestPrefix:", key);
        }

        [Fact]
        public void GenerateKey_ShouldIncludeQueryTypeName()
        {
            // Arrange
            var query = new TestQuery { Input = "test", Number = 42 };

            // Act
            var key = _generator.GenerateKey<TestQuery, string>(query);

            // Assert
            Assert.Contains("TestQuery", key);
        }

        [Fact]
        public void GenerateKey_SameQuery_ShouldReturnSameKey()
        {
            // Arrange
            var query1 = new TestQuery { Input = "test", Number = 42 };
            var query2 = new TestQuery { Input = "test", Number = 42 };

            // Act
            var key1 = _generator.GenerateKey<TestQuery, string>(query1);
            var key2 = _generator.GenerateKey<TestQuery, string>(query2);

            // Assert
            Assert.Equal(key1, key2);
        }

        [Fact]
        public void GenerateKey_DifferentQueries_ShouldReturnDifferentKeys()
        {
            // Arrange
            var query1 = new TestQuery { Input = "test1", Number = 42 };
            var query2 = new TestQuery { Input = "test2", Number = 42 };

            // Act
            var key1 = _generator.GenerateKey<TestQuery, string>(query1);
            var key2 = _generator.GenerateKey<TestQuery, string>(query2);

            // Assert
            Assert.NotEqual(key1, key2);
        }

        [Fact]
        public void GenerateKey_CustomPrefixAttribute_ShouldUseCustomPrefix()
        {
            // Arrange
            var query = new CustomPrefixQuery { Input = "test" };

            // Act
            var key = _generator.GenerateKey<CustomPrefixQuery, string>(query);

            // Assert
            Assert.StartsWith("CustomPrefix:", key);
        }

        [Fact]
        public void GenerateKey_ICacheableQuery_ShouldUseCustomCacheKey()
        {
            // Arrange
            var query = new CacheableQuery { Input = "test" };

            // Act
            var key = _generator.GenerateKey<CacheableQuery, string>(query);

            // Assert
            Assert.Contains("my-custom-key-test", key);
        }

        [Fact]
        public void GenerateKey_NullQuery_ShouldThrow()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _generator.GenerateKey<TestQuery, string>(null!));
        }

        [Fact]
        public void GenerateKey_WithoutProperties_ShouldStillWork()
        {
            // Arrange
            var generatorWithoutProps = new DefaultCacheKeyGenerator(Options.Create(new CachingOptions
            {
                CacheKeyPrefix = "Test",
                IncludeQueryPropertiesInCacheKey = false
            }));
            var query = new TestQuery { Input = "test", Number = 42 };

            // Act
            var key = generatorWithoutProps.GenerateKey<TestQuery, string>(query);

            // Assert
            Assert.Equal($"Test:{typeof(TestQuery)}", key);
        }

        public class GenericQuery<T> : IQuery<string>
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GenerateKey_SameNameQueriesInDifferentNamespaces_GetDifferentKeys(bool includeProperties)
        {
            // Arrange
            var generator = new DefaultCacheKeyGenerator(Options.Create(new CachingOptions
            {
                CacheKeyPrefix = "Test",
                IncludeQueryPropertiesInCacheKey = includeProperties
            }));

            // Act
            var keyA = generator.GenerateKey<CacheKeyNamespaceA.GetTotalQuery, int>(new CacheKeyNamespaceA.GetTotalQuery());
            var keyB = generator.GenerateKey<CacheKeyNamespaceB.GetTotalQuery, int>(new CacheKeyNamespaceB.GetTotalQuery());

            // Assert
            Assert.NotEqual(keyA, keyB);
        }

        [Fact]
        public void GenerateKey_SameNameCacheableQueriesInDifferentNamespaces_GetDifferentKeys()
        {
            // Act: both queries return the same custom CacheKey
            var keyA = _generator.GenerateKey<CacheKeyNamespaceA.GetTotalCacheableQuery, int>(new CacheKeyNamespaceA.GetTotalCacheableQuery());
            var keyB = _generator.GenerateKey<CacheKeyNamespaceB.GetTotalCacheableQuery, int>(new CacheKeyNamespaceB.GetTotalCacheableQuery());

            // Assert
            Assert.NotEqual(keyA, keyB);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GenerateKey_GenericQueriesWithDifferentArguments_GetDifferentKeys(bool includeProperties)
        {
            // Arrange
            var generator = new DefaultCacheKeyGenerator(Options.Create(new CachingOptions
            {
                CacheKeyPrefix = "Test",
                IncludeQueryPropertiesInCacheKey = includeProperties
            }));

            // Act
            var keyInt = generator.GenerateKey<GenericQuery<int>, string>(new GenericQuery<int>());
            var keyString = generator.GenerateKey<GenericQuery<string>, string>(new GenericQuery<string>());

            // Assert
            Assert.NotEqual(keyInt, keyString);
        }
    }
}

namespace Cortex.Tests.Mediator.Tests.CacheKeyNamespaceA
{
    public class GetTotalQuery : IQuery<int>
    {
    }

    public class GetTotalCacheableQuery : IQuery<int>, ICacheableQuery
    {
        public string? CacheKey => "total";
        public TimeSpan? AbsoluteExpiration => null;
        public TimeSpan? SlidingExpiration => null;
    }
}

namespace Cortex.Tests.Mediator.Tests.CacheKeyNamespaceB
{
    public class GetTotalQuery : IQuery<int>
    {
    }

    public class GetTotalCacheableQuery : IQuery<int>, ICacheableQuery
    {
        public string? CacheKey => "total";
        public TimeSpan? AbsoluteExpiration => null;
        public TimeSpan? SlidingExpiration => null;
    }
}
