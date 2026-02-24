using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Notifications
{
    /// <summary>
    /// Publishes notifications to handlers sequentially, stopping immediately on the first exception.
    /// Unlike <see cref="SequentialNotificationStrategy"/>, this strategy explicitly signals that
    /// failure handling is the primary concern — remaining handlers are intentionally skipped.
    /// </summary>
    public sealed class StopOnFirstFailureNotificationStrategy : INotificationPublishStrategy
    {
        public async Task PublishAsync(
            IEnumerable<Func<Task>> handlerDelegates,
            CancellationToken cancellationToken)
        {
            foreach (var handler in handlerDelegates)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await handler();
            }
        }
    }
}
