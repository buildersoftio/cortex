using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Linq;
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

        public async Task PublishAsync<TNotification>(
            TNotification notification,
            CancellationToken cancellationToken = default)
            where TNotification : INotification
        {
            var handlers = _serviceProvider.GetServices<INotificationHandler<TNotification>>();
            var tasks = handlers.Select(h => h.Handle(notification, cancellationToken));
            await Task.WhenAll(tasks);
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
