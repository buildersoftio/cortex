using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator
{
    /// <summary>
    /// Mediator interface for sending commands, queries, and notifications.
    /// </summary>
    public interface IMediator
    {
        Task SendAsync<TCommand>(
            TCommand command,
            CancellationToken cancellationToken = default)
            where TCommand : ICommand;

        Task<TResult> SendAsync<TQuery, TResult>(
            TQuery query,
            CancellationToken cancellationToken = default)
            where TQuery : IQuery<TResult>;

        Task PublishAsync<TNotification>(
            TNotification notification,
            CancellationToken cancellationToken = default)
            where TNotification : INotification;
    }
}
