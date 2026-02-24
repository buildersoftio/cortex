using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Notifications
{
    /// <summary>
    /// Publishes notifications to handlers one at a time in registration order.
    /// If a handler throws, the exception propagates and remaining handlers are not executed.
    /// </summary>
    public sealed class SequentialNotificationStrategy : INotificationPublishStrategy
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
