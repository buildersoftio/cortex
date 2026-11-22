using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Notifications
{
    /// <summary>
    /// Publishes notifications to all handlers in parallel using Task.WhenAll.
    /// This is the default strategy.
    /// Every handler is started even if another one throws, including before it returns a Task;
    /// the first failure is rethrown once all handlers have finished.
    /// </summary>
    public sealed class ParallelNotificationStrategy : INotificationPublishStrategy
    {
        public Task PublishAsync(
            IEnumerable<Func<Task>> handlerDelegates,
            CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();
            foreach (var handler in handlerDelegates)
            {
                Task task;
                try
                {
                    task = handler();
                }
                catch (Exception ex)
                {
                    task = Task.FromException(ex);
                }

                tasks.Add(task);
            }

            if (tasks.Count == 0)
                return Task.CompletedTask;

            return Task.WhenAll(tasks);
        }
    }
}
