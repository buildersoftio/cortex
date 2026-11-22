using Cortex.Mediator.Notifications;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior for handling exceptions in notification publishing.
    /// Provides centralized exception handling for notifications, with the option
    /// to suppress exceptions and allow other handlers to continue.
    /// </summary>
    /// <typeparam name="TNotification">The type of notification being handled.</typeparam>
    public sealed class ExceptionHandlingNotificationBehavior<TNotification> : INotificationPipelineBehavior<TNotification>
        where TNotification : INotification
    {
        private readonly ILogger<ExceptionHandlingNotificationBehavior<TNotification>> _logger;
        private readonly IExceptionHandler? _exceptionHandler;
        private readonly bool _suppressExceptions;

        /// <summary>
        /// Creates a new instance of the exception handling notification behavior.
        /// </summary>
        /// <param name="logger">The logger instance.</param>
        /// <param name="exceptionHandler">Optional custom exception handler.</param>
        /// <param name="suppressExceptions">
        /// When true, exceptions are logged but not rethrown, allowing other notification handlers to continue.
        /// When false (default), exceptions are rethrown after handling.
        /// The container cannot supply this value; configure <see cref="ExceptionHandlingOptions"/> instead.
        /// </param>
        /// <param name="options">
        /// Exception handling options from the container. Exceptions are suppressed when either
        /// <paramref name="suppressExceptions"/> or <see cref="ExceptionHandlingOptions.SuppressNotificationExceptions"/> is true.
        /// </param>
        public ExceptionHandlingNotificationBehavior(
            ILogger<ExceptionHandlingNotificationBehavior<TNotification>> logger,
            IExceptionHandler? exceptionHandler = null,
            bool suppressExceptions = false,
            IOptions<ExceptionHandlingOptions>? options = null)
        {
            _logger = logger;
            _exceptionHandler = exceptionHandler;
            _suppressExceptions = suppressExceptions || (options?.Value?.SuppressNotificationExceptions ?? false);
        }

        public async Task Handle(
            TNotification notification,
            NotificationHandlerDelegate next,
            CancellationToken cancellationToken)
        {
            try
            {
                await next();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Don't handle cancellation exceptions, just rethrow
                throw;
            }
            catch (Exception ex)
            {
                var notificationType = typeof(TNotification);
                var notificationName = notificationType.Name;

                _logger.LogError(
                    ex,
                    "Exception caught while handling notification {NotificationName}: {ExceptionMessage}",
                    notificationName,
                    ex.Message);

                if (_exceptionHandler != null)
                {
                    var handled = await _exceptionHandler.HandleAsync(
                        ex, notificationType, notification, cancellationToken);

                    if (handled)
                    {
                        _logger.LogWarning(
                            "Exception in notification {NotificationName} was handled",
                            notificationName);
                        return;
                    }
                }

                // If suppressExceptions is true, don't rethrow
                if (_suppressExceptions)
                {
                    _logger.LogWarning(
                        "Exception in notification {NotificationName} was suppressed",
                        notificationName);
                    return;
                }

                // Rethrow if not handled
                throw;
            }
        }
    }
}
