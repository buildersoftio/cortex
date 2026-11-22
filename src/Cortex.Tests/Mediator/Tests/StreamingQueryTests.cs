using Cortex.Mediator;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;
using System.Runtime.CompilerServices;

namespace Cortex.Tests.Mediator.Tests
{
    public class StreamingQueryTests
    {
        #region Test Streaming Queries and Handlers

        public class GetAllUsersStreamQuery : IStreamQuery<UserItem>
        {
            public int MaxItems { get; set; } = 10;
        }

        public class UserItem
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        public class GetAllUsersStreamQueryHandler : IStreamQueryHandler<GetAllUsersStreamQuery, UserItem>
        {
            public int StartCallCount { get; private set; }

            public async IAsyncEnumerable<UserItem> Handle(
                GetAllUsersStreamQuery query, 
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                StartCallCount++;

                for (int i = 1; i <= query.MaxItems; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    // Simulate async database fetch
                    await Task.Delay(1, cancellationToken);
                    
                    yield return new UserItem
                    {
                        Id = i,
                        Name = $"User-{i}"
                    };
                }
            }
        }

        public class EmptyStreamQuery : IStreamQuery<string>
        {
        }

        public class EmptyStreamQueryHandler : IStreamQueryHandler<EmptyStreamQuery, string>
        {
            public async IAsyncEnumerable<string> Handle(
                EmptyStreamQuery query,
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                await Task.CompletedTask;
                yield break;
            }
        }

        public class ThrowingStreamQuery : IStreamQuery<int>
        {
            public int ThrowAfterItems { get; set; } = 3;
        }

        public class ThrowingStreamQueryHandler : IStreamQueryHandler<ThrowingStreamQuery, int>
        {
            public async IAsyncEnumerable<int> Handle(
                ThrowingStreamQuery query,
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                for (int i = 1; i <= 10; i++)
                {
                    await Task.Delay(1, cancellationToken);
                    
                    if (i > query.ThrowAfterItems)
                    {
                        throw new InvalidOperationException($"Error after item {query.ThrowAfterItems}");
                    }
                    
                    yield return i;
                }
            }
        }

        #endregion

        private IServiceProvider CreateServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddSingleton<GetAllUsersStreamQueryHandler>();
            services.AddTransient<IStreamQueryHandler<GetAllUsersStreamQuery, UserItem>>(sp =>
                sp.GetRequiredService<GetAllUsersStreamQueryHandler>());
            services.AddTransient<IStreamQueryHandler<EmptyStreamQuery, string>, EmptyStreamQueryHandler>();
            services.AddTransient<IStreamQueryHandler<ThrowingStreamQuery, int>, ThrowingStreamQueryHandler>();

            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task CreateStream_ShouldReturnAllItems()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new GetAllUsersStreamQuery { MaxItems = 5 };

            // Act
            var items = new List<UserItem>();
            await foreach (var item in mediator.CreateStream<GetAllUsersStreamQuery, UserItem>(query))
            {
                items.Add(item);
            }

            // Assert
            Assert.Equal(5, items.Count);
            Assert.Equal("User-1", items[0].Name);
            Assert.Equal("User-5", items[4].Name);
        }

        [Fact]
        public async Task CreateStream_WithTypeInference_ShouldWork()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new GetAllUsersStreamQuery { MaxItems = 3 };

            // Act
            var items = new List<UserItem>();
            await foreach (var item in mediator.CreateStream<UserItem>(query))
            {
                items.Add(item);
            }

            // Assert
            Assert.Equal(3, items.Count);
        }

        [Fact]
        public async Task StreamAsync_ExtensionMethod_ShouldWork()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new GetAllUsersStreamQuery { MaxItems = 4 };

            // Act
            var items = new List<UserItem>();
            await foreach (var item in mediator.StreamAsync(query))
            {
                items.Add(item);
            }

            // Assert
            Assert.Equal(4, items.Count);
        }

        [Fact]
        public async Task CreateStream_EmptyStream_ShouldReturnNoItems()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new EmptyStreamQuery();

            // Act
            var items = new List<string>();
            await foreach (var item in mediator.StreamAsync(query))
            {
                items.Add(item);
            }

            // Assert
            Assert.Empty(items);
        }

        [Fact]
        public async Task CreateStream_WhenCancelled_ShouldStopEnumeration()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new GetAllUsersStreamQuery { MaxItems = 100 };
            var cts = new CancellationTokenSource();

            // Act
            var items = new List<UserItem>();
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (var item in mediator.CreateStream<GetAllUsersStreamQuery, UserItem>(query, cts.Token))
                {
                    items.Add(item);
                    if (items.Count >= 3)
                    {
                        cts.Cancel();
                    }
                }
            });

            // Assert
            Assert.Equal(3, items.Count);
        }

        [Fact]
        public async Task CreateStream_WhenHandlerThrows_ShouldPropagateException()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var query = new ThrowingStreamQuery { ThrowAfterItems = 2 };

            // Act & Assert
            var items = new List<int>();
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var item in mediator.StreamAsync(query))
                {
                    items.Add(item);
                }
            });

            Assert.Equal(2, items.Count);
            Assert.Contains("Error after item 2", exception.Message);
        }

        [Fact]
        public async Task CreateStream_ShouldOnlyCallHandlerOnce()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();
            var handler = provider.GetRequiredService<GetAllUsersStreamQueryHandler>();
            var query = new GetAllUsersStreamQuery { MaxItems = 5 };

            // Act
            await foreach (var _ in mediator.StreamAsync(query))
            {
                // Consume all items
            }

            // Assert
            Assert.Equal(1, handler.StartCallCount);
        }

        [Fact]
        public void CreateStream_NullQuery_ShouldThrow()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                mediator.CreateStream<GetAllUsersStreamQuery, UserItem>(null!));
        }

        [Fact]
        public void CreateStream_WithTypeInference_NullQuery_ShouldThrow()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                mediator.CreateStream<UserItem>(null!));
        }

        public class UnhandledStreamQuery : IStreamQuery<int>
        {
        }

        public class GuardedStreamQuery : IStreamQuery<int>
        {
        }

        public class GuardedStreamQueryHandler : IStreamQueryHandler<GuardedStreamQuery, int>
        {
            public async IAsyncEnumerable<int> Handle(
                GuardedStreamQuery query,
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                await Task.Yield();
                yield return 1;
            }
        }

        // Throws while the pipeline is being built, before any enumeration.
        public class ThrowingStreamBehavior : IStreamQueryPipelineBehavior<GuardedStreamQuery, int>
        {
            public IAsyncEnumerable<int> Handle(
                GuardedStreamQuery query,
                StreamQueryHandlerDelegate<int> next,
                CancellationToken cancellationToken)
            {
                throw new NotSupportedException("guard failed");
            }
        }

        [Fact]
        public void CreateStream_RuntimeTyped_WithoutHandler_ThrowsInvalidOperationException()
        {
            // Arrange
            var provider = CreateServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            // Act & Assert: the DI error itself, not a reflection wrapper
            Assert.Throws<InvalidOperationException>(() =>
                mediator.CreateStream(new UnhandledStreamQuery()));
        }

        [Fact]
        public void CreateStream_RuntimeTyped_BehaviorThrowsSynchronously_ThrowsOriginalException()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddSingleton<IMediator, Cortex.Mediator.Mediator>();
            services.AddTransient<IStreamQueryHandler<GuardedStreamQuery, int>, GuardedStreamQueryHandler>();
            services.AddTransient<IStreamQueryPipelineBehavior<GuardedStreamQuery, int>, ThrowingStreamBehavior>();
            var mediator = services.BuildServiceProvider().GetRequiredService<IMediator>();

            // Act
            var ex = Assert.Throws<NotSupportedException>(() =>
                mediator.CreateStream(new GuardedStreamQuery()));

            // Assert: the original stack trace is kept
            Assert.Equal("guard failed", ex.Message);
            Assert.Contains(nameof(ThrowingStreamBehavior), ex.StackTrace);
        }
    }
}
