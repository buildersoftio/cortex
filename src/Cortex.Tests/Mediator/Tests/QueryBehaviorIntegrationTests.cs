using Cortex.Mediator;
using Cortex.Mediator.DependencyInjection;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Query Behavior Integration Tests

    public class IntegrationTestQuery : IQuery<string>
    {
        public string Data { get; set; } = string.Empty;
    }

    public class IntegrationQueryHandler : IQueryHandler<IntegrationTestQuery, string>
    {
        public Task<string> Handle(IntegrationTestQuery query, CancellationToken cancellationToken)
        {
            return Task.FromResult($"Result: {query.Data}");
        }
    }

    public class OpenGenericQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        public static List<string> ExecutionLog { get; } = new();

        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            ExecutionLog.Add($"Before: {typeof(TQuery).Name}");
            var result = await next();
            ExecutionLog.Add($"After: {typeof(TQuery).Name}");
            return result;
        }
    }

    public class ClosedQueryBehavior : IQueryPipelineBehavior<IntegrationTestQuery, string>
    {
        public static List<string> ExecutionLog { get; } = new();

        public async Task<string> Handle(IntegrationTestQuery query, QueryHandlerDelegate<string> next, CancellationToken cancellationToken)
        {
            ExecutionLog.Add($"ClosedBehavior Before: {query.Data}");
            var result = await next();
            ExecutionLog.Add($"ClosedBehavior After: {query.Data}");
            return result;
        }
    }

    #endregion

    public class QueryBehaviorIntegrationTests
    {
        [Fact]
        public void AddOpenQueryPipelineBehavior_ShouldRegisterBehavior()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddOpenQueryPipelineBehavior(typeof(OpenGenericQueryBehavior<,>));
                });

            var provider = services.BuildServiceProvider();

            // Assert
            var behaviors = provider.GetServices<IQueryPipelineBehavior<IntegrationTestQuery, string>>();
            Assert.Single(behaviors);
        }

        [Fact]
        public void AddQueryPipelineBehavior_Closed_ShouldRegisterBehavior()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddQueryPipelineBehavior<ClosedQueryBehavior>();
                });

            var provider = services.BuildServiceProvider();

            // Assert
            var behaviors = provider.GetServices<IQueryPipelineBehavior<IntegrationTestQuery, string>>();
            Assert.Single(behaviors);
        }

        [Fact]
        public void AddQueryPipelineBehavior_ClosedGeneric_ShouldRegisterSuccessfully()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act - Should NOT throw because OpenGenericQueryBehavior<IntegrationTestQuery, string>
            // is a closed generic type, not an open generic definition
            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddQueryPipelineBehavior<OpenGenericQueryBehavior<IntegrationTestQuery, string>>();
                });

            var provider = services.BuildServiceProvider();

            // Assert - The behavior should be registered
            var behaviors = provider.GetServices<IQueryPipelineBehavior<IntegrationTestQuery, string>>();
            Assert.Single(behaviors);
        }

        [Fact]
        public void AddOpenQueryPipelineBehavior_NonOpenGeneric_ShouldThrowArgumentException()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                options.AddOpenQueryPipelineBehavior(typeof(ClosedQueryBehavior)));
        }

        [Fact]
        public void AddOpenQueryPipelineBehavior_NonBehaviorType_ShouldThrowArgumentException()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                options.AddOpenQueryPipelineBehavior(typeof(List<>)));
        }

        [Fact]
        public void AddQueryPipelineBehavior_ClosedConstructedGeneric_ShouldNotThrow()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act - OpenGenericQueryBehavior<IntegrationTestQuery, string> is a closed constructed
            // generic (not an open generic definition), so it should be accepted
            options.AddQueryPipelineBehavior<OpenGenericQueryBehavior<IntegrationTestQuery, string>>();

            // Assert - no exception thrown
        }

        [Fact]
        public void AddQueryPipelineBehavior_NonBehaviorType_ShouldThrowArgumentException()
        {
            // Arrange
            var options = new MediatorOptions();

            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                options.AddQueryPipelineBehavior<string>());
        }

        [Fact]
        public async Task IntegrationTest_WithOpenGenericBehavior_ShouldExecuteBehavior()
        {
            // Arrange
            var services = new ServiceCollection();

            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddOpenQueryPipelineBehavior(typeof(OpenGenericQueryBehavior<,>));
                });

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            var query = new IntegrationTestQuery { Data = "OpenGenericTest" };

            // Act
            OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog.Clear();
            var result = await mediator.QueryAsync(query);

            // Assert
            Assert.Equal("Result: OpenGenericTest", result);
            Assert.Contains("Before: IntegrationTestQuery", OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog);
            Assert.Contains("After: IntegrationTestQuery", OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog);
        }

        [Fact]
        public async Task IntegrationTest_WithClosedBehavior_ShouldExecuteBehavior()
        {
            // Arrange
            var services = new ServiceCollection();

            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddQueryPipelineBehavior<ClosedQueryBehavior>();
                });

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            var query = new IntegrationTestQuery { Data = "ClosedTest" };

            // Act
            ClosedQueryBehavior.ExecutionLog.Clear();
            var result = await mediator.QueryAsync(query);

            // Assert
            Assert.Equal("Result: ClosedTest", result);
            Assert.Contains("ClosedBehavior Before: ClosedTest", ClosedQueryBehavior.ExecutionLog);
            Assert.Contains("ClosedBehavior After: ClosedTest", ClosedQueryBehavior.ExecutionLog);
        }

        [Fact]
        public async Task IntegrationTest_WithBothOpenAndClosedBehaviors_ShouldExecuteBoth()
        {
            // Arrange
            var services = new ServiceCollection();

            services.AddCortexMediator(
                new[] { typeof(IntegrationQueryHandler) },
                options =>
                {
                    options.AddOpenQueryPipelineBehavior(typeof(OpenGenericQueryBehavior<,>));
                    options.AddQueryPipelineBehavior<ClosedQueryBehavior>();
                });

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            var query = new IntegrationTestQuery { Data = "BothTest" };

            // Act
            OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog.Clear();
            ClosedQueryBehavior.ExecutionLog.Clear();
            var result = await mediator.QueryAsync(query);

            // Assert
            Assert.Equal("Result: BothTest", result);

            // Both behaviors should have executed
            Assert.Contains("Before: IntegrationTestQuery", OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog);
            Assert.Contains("After: IntegrationTestQuery", OpenGenericQueryBehavior<IntegrationTestQuery, string>.ExecutionLog);
            Assert.Contains("ClosedBehavior Before: BothTest", ClosedQueryBehavior.ExecutionLog);
            Assert.Contains("ClosedBehavior After: BothTest", ClosedQueryBehavior.ExecutionLog);
        }
    }
}
