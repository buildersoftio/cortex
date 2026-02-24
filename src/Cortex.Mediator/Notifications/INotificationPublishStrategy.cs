using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Notifications
{
    /// <summary>
    /// Defines the strategy for publishing notifications to multiple handler pipelines.
    /// </summary>
    public interface INotificationPublishStrategy
    {
        /// <summary>
        /// Publishes a notification by executing the provided handler delegates according to the strategy.
        /// </summary>
        /// <param name="handlerDelegates">The handler pipeline delegates to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task PublishAsync(
            IEnumerable<Func<Task>> handlerDelegates,
            CancellationToken cancellationToken);
    }
}
