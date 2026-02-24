using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator
{
    /// <summary>
    /// Default implementation of the IMediator interface.
    /// </summary>
    public class Mediator : IMediator
    {
        private readonly IServiceProvider _serviceProvider;

        // Cache for reflection-based method lookups to improve performance
        private static readonly ConcurrentDictionary<Type, MethodInfo> _sendCommandMethodCache = new();
        private static readonly ConcurrentDictionary<Type, MethodInfo> _sendQueryMethodCache = new();
        private static readonly ConcurrentDictionary<Type, MethodInfo> _sendVoidCommandMethodCache = new();
        private static readonly ConcurrentDictionary<Type, MethodInfo> _createStreamMethodCache = new();
        private static readonly ConcurrentDictionary<Type, MethodInfo> _publishMethodCache = new();

        public Mediator(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public async Task<TResult> SendCommandAsync<TCommand, TResult>(TCommand command, CancellationToken cancellationToken = default)
            where TCommand : ICommand<TResult>
        {
            var handler = _serviceProvider.GetRequiredService<ICommandHandler<TCommand, TResult>>();

            foreach (var behavior in _serviceProvider.GetServices<ICommandPipelineBehavior<TCommand, TResult>>().Reverse())
            {
                handler = new PipelineBehaviorNextDelegate<TCommand, TResult>(behavior, handler);
            }

            return await handler.Handle(command, cancellationToken);
        }

        public Task<TResult> SendCommandAsync<TResult>(ICommand<TResult> command, CancellationToken cancellationToken = default)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));

            var commandType = command.GetType();
            var resultType = typeof(TResult);

            var method = _sendCommandMethodCache.GetOrAdd(commandType, type =>
            {
                var genericMethod = typeof(Mediator)
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == nameof(SendCommandAsync) &&
                                m.IsGenericMethodDefinition &&
                                m.GetGenericArguments().Length == 2);

                return genericMethod.MakeGenericMethod(type, resultType);
            });

            return (Task<TResult>)method.Invoke(this, new object[] { command, cancellationToken })!;
        }

        public async Task SendCommandAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default) where TCommand : ICommand
        {
            var handler = _serviceProvider.GetRequiredService<ICommandHandler<TCommand>>();

            foreach (var behavior in _serviceProvider.GetServices<ICommandPipelineBehavior<TCommand>>().Reverse())
            {
                handler = new PipelineBehaviorNextDelegate<TCommand>(behavior, handler);
            }

            await handler.Handle(command, cancellationToken);
        }

        public Task SendCommandAsync(ICommand command, CancellationToken cancellationToken = default)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));

            var commandType = command.GetType();

            var method = _sendVoidCommandMethodCache.GetOrAdd(commandType, type =>
            {
                var genericMethod = typeof(Mediator)
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == nameof(SendCommandAsync) &&
                                m.IsGenericMethodDefinition &&
                                m.GetGenericArguments().Length == 1 &&
                                m.GetParameters().Length == 2 &&
                                m.GetParameters()[0].ParameterType.IsGenericParameter);

                return genericMethod.MakeGenericMethod(type);
            });

            return (Task)method.Invoke(this, new object[] { command, cancellationToken })!;
        }

        public async Task<TResult> SendQueryAsync<TQuery, TResult>(TQuery query, CancellationToken cancellationToken = default)
            where TQuery : IQuery<TResult>
        {
            var handler = _serviceProvider.GetRequiredService<IQueryHandler<TQuery, TResult>>();

            foreach (var behavior in _serviceProvider.GetServices<IQueryPipelineBehavior<TQuery, TResult>>().Reverse())
            {
                handler = new QueryPipelineBehaviorNextDelegate<TQuery, TResult>(behavior, handler);
            }

            return await handler.Handle(query, cancellationToken);
        }

        public Task<TResult> SendQueryAsync<TResult>(IQuery<TResult> query, CancellationToken cancellationToken = default)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var queryType = query.GetType();
            var resultType = typeof(TResult);

            var method = _sendQueryMethodCache.GetOrAdd(queryType, type =>
            {
                var genericMethod = typeof(Mediator)
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == nameof(SendQueryAsync) &&
                                m.IsGenericMethodDefinition &&
                                m.GetGenericArguments().Length == 2);

                return genericMethod.MakeGenericMethod(type, resultType);
            });

            return (Task<TResult>)method.Invoke(this, new object[] { query, cancellationToken })!;
        }

        public async Task PublishAsync<TNotification>(
            TNotification notification,
            CancellationToken cancellationToken = default)
            where TNotification : INotification
        {
            var handlers = _serviceProvider.GetServices<INotificationHandler<TNotification>>();
            var behaviors = _serviceProvider.GetServices<INotificationPipelineBehavior<TNotification>>();

            // Materialize behaviors once since we need to iterate multiple times
            // Use stackalloc-friendly pattern for small counts
            var behaviorList = behaviors as INotificationPipelineBehavior<TNotification>[] ?? behaviors.ToArray();
            Array.Reverse(behaviorList);

            // Count handlers to pre-allocate task array
            var handlerList = handlers as INotificationHandler<TNotification>[] ?? handlers.ToArray();
            if (handlerList.Length == 0)
                return;

            var tasks = new Task[handlerList.Length];
            for (int i = 0; i < handlerList.Length; i++)
            {
                var handler = handlerList[i];
                // Build the pipeline for this specific handler
                NotificationHandlerDelegate handlerDelegate = () => handler.Handle(notification, cancellationToken);

                // Wrap the handler with all behaviors (already reversed)
                foreach (var behavior in behaviorList)
                {
                    var currentDelegate = handlerDelegate;
                    var currentBehavior = behavior;
                    handlerDelegate = () => currentBehavior.Handle(notification, currentDelegate, cancellationToken);
                }

                tasks[i] = handlerDelegate();
            }

            await Task.WhenAll(tasks);
        }

        public Task PublishAsync(INotification notification, CancellationToken cancellationToken = default)
        {
            if (notification == null)
                throw new ArgumentNullException(nameof(notification));

            var notificationType = notification.GetType();

            var method = _publishMethodCache.GetOrAdd(notificationType, type =>
            {
                var genericMethod = typeof(Mediator)
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == nameof(PublishAsync) &&
                                m.IsGenericMethodDefinition &&
                                m.GetGenericArguments().Length == 1);

                return genericMethod.MakeGenericMethod(type);
            });

            return (Task)method.Invoke(this, new object[] { notification, cancellationToken })!;
        }

        public IAsyncEnumerable<TResult> CreateStream<TQuery, TResult>(
            TQuery query,
            CancellationToken cancellationToken = default)
            where TQuery : IStreamQuery<TResult>
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var handler = _serviceProvider.GetRequiredService<IStreamQueryHandler<TQuery, TResult>>();
            var behaviors = _serviceProvider.GetServices<IStreamQueryPipelineBehavior<TQuery, TResult>>();

            // Build the pipeline
            StreamQueryHandlerDelegate<TResult> handlerDelegate = () => handler.Handle(query, cancellationToken);

            // Materialize and reverse behaviors
            var behaviorArray = behaviors as IStreamQueryPipelineBehavior<TQuery, TResult>[] ?? behaviors.ToArray();
            for (int i = behaviorArray.Length - 1; i >= 0; i--)
            {
                var currentDelegate = handlerDelegate;
                var currentBehavior = behaviorArray[i];
                handlerDelegate = () => currentBehavior.Handle(query, currentDelegate, cancellationToken);
            }

            return handlerDelegate();
        }

        public IAsyncEnumerable<TResult> CreateStream<TResult>(
            IStreamQuery<TResult> query,
            CancellationToken cancellationToken = default)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var queryType = query.GetType();
            var resultType = typeof(TResult);

            var method = _createStreamMethodCache.GetOrAdd(queryType, type =>
            {
                var genericMethod = typeof(Mediator)
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == nameof(CreateStream) &&
                                m.IsGenericMethodDefinition &&
                                m.GetGenericArguments().Length == 2);

                return genericMethod.MakeGenericMethod(type, resultType);
            });

            return (IAsyncEnumerable<TResult>)method.Invoke(this, new object[] { query, cancellationToken })!;
        }

    
        private class PipelineBehaviorNextDelegate<TCommand, TResult> : ICommandHandler<TCommand, TResult>
        where TCommand : ICommand<TResult>
        {
            private readonly ICommandPipelineBehavior<TCommand, TResult> _behavior;
            private readonly ICommandHandler<TCommand, TResult> _next;

            public PipelineBehaviorNextDelegate(
                ICommandPipelineBehavior<TCommand, TResult> behavior,
                ICommandHandler<TCommand, TResult> next)
            {
                _behavior = behavior;
                _next = next;
            }

            public Task<TResult> Handle(TCommand command, CancellationToken cancellationToken)
            {
                return _behavior.Handle(
                    command,
                    () => _next.Handle(command, cancellationToken),
                    cancellationToken);
            }
        }

        private class PipelineBehaviorNextDelegate<TCommand> : ICommandHandler<TCommand>
            where TCommand : ICommand
        {
            private readonly ICommandPipelineBehavior<TCommand> _behavior;
            private readonly ICommandHandler<TCommand> _next;

            public PipelineBehaviorNextDelegate(
                ICommandPipelineBehavior<TCommand> behavior,
                ICommandHandler<TCommand> next)
            {
                _behavior = behavior;
                _next = next;
            }

            public Task Handle(TCommand command, CancellationToken cancellationToken)
            {
                return _behavior.Handle(
                    command,
                    () => _next.Handle(command, cancellationToken),
                    cancellationToken);
            }
        }

        private class QueryPipelineBehaviorNextDelegate<TQuery, TResult>
       : IQueryHandler<TQuery, TResult>
       where TQuery : IQuery<TResult>
        {
            private readonly IQueryPipelineBehavior<TQuery, TResult> _behavior;
            private readonly IQueryHandler<TQuery, TResult> _next;

            public QueryPipelineBehaviorNextDelegate(
                IQueryPipelineBehavior<TQuery, TResult> behavior,
                IQueryHandler<TQuery, TResult> next)
            {
                _behavior = behavior;
                _next = next;
            }

            public Task<TResult> Handle(TQuery query, CancellationToken cancellationToken)
            {
                return _behavior.Handle(
                    query,
                    () => _next.Handle(query, cancellationToken),
                    cancellationToken);
            }
        }
    }
}
