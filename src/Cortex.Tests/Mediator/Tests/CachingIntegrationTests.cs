using Cortex.Mediator;
using Cortex.Mediator.Caching;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cortex.Tests.Mediator.Tests
{
    public class CachingIntegrationTests
    {
        #region Test Queries and Handlers

        [Cacheable(AbsoluteExpirationSeconds = 300)]
        public class GetUserQuery : IQuery<UserDto>
        {
            public int UserId { get; set; }
        }

        public class UserDto
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        public class GetUserQueryHandler : IQueryHandler<GetUserQuery, UserDto>
        {
            public int ExecutionCount { get; private set; }

            public Task<UserDto> Handle(GetUserQuery query, CancellationToken cancellationToken)
            {
                ExecutionCount++;
                return Task.FromResult(new UserDto
                {
                    Id = query.UserId,
                    Name = $"User-{query.UserId}"
                });
            }
        }

        public class GetProductQuery : IQuery<ProductDto>, ICacheableQuery
        {
            public int ProductId { get; set; }
            public string? CacheKey => $"product-{ProductId}";
            public TimeSpan? AbsoluteExpiration => TimeSpan.FromMinutes(10);
            public TimeSpan? SlidingExpiration => TimeSpan.FromMinutes(2);
        }

        public class ProductDto
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        public class GetProductQueryHandler : IQueryHandler<GetProductQuery, ProductDto>
        {
            public int ExecutionCount { get; private set; }

            public Task<ProductDto> Handle(GetProductQuery query, CancellationToken cancellationToken)
            {
                ExecutionCount++;
                return Task.FromResult(new ProductDto
                {
                    Id = query.ProductId,
                    Name = $"Product-{query.ProductId}"
                });
            }
        }

        // Non-cacheable query
        public class GetOrderQuery : IQuery<string>
        {
            public int OrderId { get; set; }
        }

        public class GetOrderQueryHandler : IQueryHandler<GetOrderQuery, string>
        {
            public int ExecutionCount { get; private set; }

            public Task<string> Handle(GetOrderQuery query, CancellationToken cancellationToken)
            {
                ExecutionCount++;
                return Task.FromResult($"Order-{query.OrderId}");
            }
        }

        #endregion

        private IServiceProvider CreateServiceProvider()
        {
            var services = new ServiceCollection();

            services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

            // Add caching services
            services.AddMediatorCaching(options =>
            {
                options.CacheKeyPrefix = "IntegrationTest";
                options.DefaultAbsoluteExpiration = TimeSpan.FromMinutes(5);
            });

            // Use empty array to avoid assembly scanning
            services.AddCortexMediator(
                Array.Empty<Type>(),
                options => options.AddCachingBehavior());

            // Register handlers as singletons so we can track execution count
            services.AddSingleton<GetUserQueryHandler>();
            services.AddSingleton<GetProductQueryHandler>();
            services.AddSingleton<GetOrderQueryHandler>();

            services.AddTransient<IQueryHandler<GetUserQuery, UserDto>>(sp =>
                sp.GetRequiredService<GetUserQueryHandler>());
            services.AddTransient<IQueryHandler<GetProductQuery, ProductDto>>(sp =>
                sp.GetRequiredService<GetProductQueryHandler>());
            services.AddTransient<IQueryHandler<GetOrderQuery, string>>(sp =>
                sp.GetRequiredService<GetOrderQueryHandler>());

            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task Query_WithCacheableAttribute_ShouldBeCached()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetUserQueryHandler>();

            // Act
            var result1 = await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            var result2 = await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            var result3 = await mediator.QueryAsync(new GetUserQuery { UserId = 2 });

            // Assert
            Assert.Equal("User-1", result1.Name);
            Assert.Equal("User-1", result2.Name);
            Assert.Equal("User-2", result3.Name);
            Assert.Equal(2, handler.ExecutionCount); // 1 and 2 cached, but 2 is different
        }

        [Fact]
        public async Task Query_WithICacheableQuery_ShouldBeCached()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetProductQueryHandler>();

            // Act
            var result1 = await mediator.QueryAsync(new GetProductQuery { ProductId = 100 });
            var result2 = await mediator.QueryAsync(new GetProductQuery { ProductId = 100 });

            // Assert
            Assert.Equal("Product-100", result1.Name);
            Assert.Equal("Product-100", result2.Name);
            Assert.Equal(1, handler.ExecutionCount); // Should only execute once
        }

        [Fact]
        public async Task Query_NonCacheable_ShouldNotBeCached()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetOrderQueryHandler>();

            // Act
            var result1 = await mediator.QueryAsync(new GetOrderQuery { OrderId = 1 });
            var result2 = await mediator.QueryAsync(new GetOrderQuery { OrderId = 1 });

            // Assert
            Assert.Equal("Order-1", result1);
            Assert.Equal("Order-1", result2);
            Assert.Equal(2, handler.ExecutionCount); // Should execute twice - no caching
        }

        [Fact]
        public async Task CacheInvalidation_ShouldRemoveCachedResult()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetUserQueryHandler>();
            var invalidator = provider.GetRequiredService<ICacheInvalidator>();

            var query = new GetUserQuery { UserId = 5 };

            // Act - First call should cache
            var result1 = await mediator.QueryAsync(query);
            Assert.Equal(1, handler.ExecutionCount);

            // Invalidate cache
            invalidator.Invalidate<GetUserQuery, UserDto>(query);

            // Second call should execute handler again
            var result2 = await mediator.QueryAsync(query);

            // Assert
            Assert.Equal("User-5", result1.Name);
            Assert.Equal("User-5", result2.Name);
            Assert.Equal(2, handler.ExecutionCount); // Should execute twice due to invalidation
        }

        [Fact]
        public async Task InvalidateAll_RemovesEveryCachedResultOfThatQueryType()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var userHandler = provider.GetRequiredService<GetUserQueryHandler>();
            var productHandler = provider.GetRequiredService<GetProductQueryHandler>();
            var invalidator = provider.GetRequiredService<ICacheInvalidator>();

            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            await mediator.QueryAsync(new GetUserQuery { UserId = 2 });
            await mediator.QueryAsync(new GetProductQuery { ProductId = 1 });

            // Act
            invalidator.InvalidateAll<GetUserQuery, UserDto>();

            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            await mediator.QueryAsync(new GetUserQuery { UserId = 2 });
            await mediator.QueryAsync(new GetProductQuery { ProductId = 1 });

            // Assert: both user queries ran again; the product query is still cached
            Assert.Equal(4, userHandler.ExecutionCount);
            Assert.Equal(1, productHandler.ExecutionCount);
        }

        [Fact]
        public async Task InvalidateAll_ThenNewResults_AreCachedAgain()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetUserQueryHandler>();
            var invalidator = provider.GetRequiredService<ICacheInvalidator>();

            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            invalidator.InvalidateAll<GetUserQuery, UserDto>();

            // Act
            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });

            // Assert: one run before, one after invalidation, then a cache hit
            Assert.Equal(2, handler.ExecutionCount);
        }

        [Fact]
        public void AddMediatorCaching_ShouldRegisterServices()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddMediatorCaching(options =>
            {
                options.CacheKeyPrefix = "Test";
            });

            var provider = services.BuildServiceProvider();

            // Assert
            Assert.NotNull(provider.GetService<IMemoryCache>());
            Assert.NotNull(provider.GetService<ICacheKeyGenerator>());
            Assert.NotNull(provider.GetService<ICacheInvalidator>());
        }

        [Fact]
        public async Task CachingWithDisabledOption_ShouldBypassCache()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory, NullLoggerFactory>();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

            services.AddMediatorCaching(options =>
            {
                options.EnableCaching = false; // Disable caching
            });

            services.AddCortexMediator(
                Array.Empty<Type>(),
                options => options.AddCachingBehavior());

            services.AddSingleton<GetUserQueryHandler>();
            services.AddTransient<IQueryHandler<GetUserQuery, UserDto>>(sp =>
                sp.GetRequiredService<GetUserQueryHandler>());

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetUserQueryHandler>();

            // Act
            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });
            await mediator.QueryAsync(new GetUserQuery { UserId = 1 });

            // Assert
            Assert.Equal(2, handler.ExecutionCount); // Should execute twice when disabled
        }
    }
}
