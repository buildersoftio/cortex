using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Notifications
{
    /// <summary>
    /// Publishes notifications to all handlers in parallel using Task.WhenAll.
    /// This is the default strategy.
    /// </summary>
    public sealed class ParallelNotificationStrategy : INotificationPublishStrategy
    {
        public Task PublishAsync(
            IEnumerable<Func<Task>> handlerDelegates,
            CancellationToken cancellationToken)
        {
            var tasks = handlerDelegates.Select(handler => handler()).ToArray();

            if (tasks.Length == 0)
                return Task.CompletedTask;

            return Task.WhenAll(tasks);
        }
    }
}
