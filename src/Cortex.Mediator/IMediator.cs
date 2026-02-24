using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator
{
    /// <summary>
    /// Mediator interface for sending commands, queries, and notifications.
    /// </summary>
    public interface IMediator
    {
        /// <summary>
        /// Sends a command with explicit type parameters.
        /// </summary>
        /// <typeparam name="TCommand">The type of command being sent.</typeparam>
        /// <typeparam name="TResult">The type of result returned by the command.</typeparam>
        /// <param name="command">The command to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the command execution.</returns>
        Task<TResult> SendCommandAsync<TCommand, TResult>(
            TCommand command,
            CancellationToken cancellationToken = default)
            where TCommand : ICommand<TResult>;

        /// <summary>
        /// Sends a command that returns a result. The result type is inferred from the command interface.
        /// </summary>
        /// <typeparam name="TResult">The type of result returned by the command (inferred from ICommand&lt;TResult&gt;).</typeparam>
        /// <param name="command">The command to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the command execution.</returns>
        Task<TResult> SendCommandAsync<TResult>(
            ICommand<TResult> command,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Sends a command with explicit type parameter that does not return a value.
        /// </summary>
        /// <typeparam name="TCommand">The type of command being sent.</typeparam>
        /// <param name="command">The command to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task SendCommandAsync<TCommand>(
            TCommand command,
            CancellationToken cancellationToken = default)
            where TCommand : ICommand;

        /// <summary>
        /// Sends a command that does not return a value.
        /// </summary>
        /// <param name="command">The command to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task SendCommandAsync(
            ICommand command,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Sends a query with explicit type parameters.
        /// </summary>
        /// <typeparam name="TQuery">The type of query being sent.</typeparam>
        /// <typeparam name="TResult">The type of result returned by the query.</typeparam>
        /// <param name="query">The query to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the query execution.</returns>
        Task<TResult> SendQueryAsync<TQuery, TResult>(
            TQuery query,
            CancellationToken cancellationToken = default)
            where TQuery : IQuery<TResult>;

        /// <summary>
        /// Sends a query that returns a result. The result type is inferred from the query interface.
        /// </summary>
        /// <typeparam name="TResult">The type of result returned by the query (inferred from IQuery&lt;TResult&gt;).</typeparam>
        /// <param name="query">The query to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the query execution.</returns>
        Task<TResult> SendQueryAsync<TResult>(
            IQuery<TResult> query,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Sends a streaming query with explicit type parameters.
        /// Returns an asynchronous enumerable that yields results one at a time.
        /// </summary>
        /// <typeparam name="TQuery">The type of streaming query being sent.</typeparam>
        /// <typeparam name="TResult">The type of each item in the result stream.</typeparam>
        /// <param name="query">The streaming query to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An asynchronous enumerable of results.</returns>
        IAsyncEnumerable<TResult> CreateStream<TQuery, TResult>(
            TQuery query,
            CancellationToken cancellationToken = default)
            where TQuery : IStreamQuery<TResult>;

        /// <summary>
        /// Sends a streaming query. The result type is inferred from the query interface.
        /// Returns an asynchronous enumerable that yields results one at a time.
        /// </summary>
        /// <typeparam name="TResult">The type of each item in the result stream.</typeparam>
        /// <param name="query">The streaming query to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An asynchronous enumerable of results.</returns>
        IAsyncEnumerable<TResult> CreateStream<TResult>(
            IStreamQuery<TResult> query,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Publishes a notification to all registered handlers.
        /// </summary>
        /// <typeparam name="TNotification">The type of notification being published.</typeparam>
        /// <param name="notification">The notification to publish.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PublishAsync<TNotification>(
            TNotification notification,
            CancellationToken cancellationToken = default)
            where TNotification : INotification;

        /// <summary>
        /// Publishes a notification to all registered handlers.
        /// The notification type is resolved at runtime, enabling scenarios where the concrete
        /// type is not known at compile time (e.g., dispatching domain events from a collection,
        /// deserializing events from a message bus).
        /// </summary>
        /// <param name="notification">The notification to publish.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PublishAsync(
            INotification notification,
            CancellationToken cancellationToken = default);
    }
}
