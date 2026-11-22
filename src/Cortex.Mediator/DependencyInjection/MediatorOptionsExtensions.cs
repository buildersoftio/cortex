using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Processors;

namespace Cortex.Mediator.DependencyInjection
{
    public static class MediatorOptionsExtensions
    {
        /// <summary>
        /// Adds default logging behaviors for commands, queries, and notifications.
        /// </summary>
        public static MediatorOptions AddDefaultBehaviors(this MediatorOptions options)
        {
            return options
                // Register the open generic logging behavior for commands that return TResult
                .AddOpenCommandPipelineBehavior(typeof(LoggingCommandBehavior<,>))
                .AddOpenQueryPipelineBehavior(typeof(LoggingQueryBehavior<,>))
                .AddOpenCommandPipelineBehavior(typeof(LoggingCommandBehavior<>)) // Add void command logging
                .AddOpenNotificationPipelineBehavior(typeof(LoggingNotificationBehavior<>)); // Add notification logging
        }

        /// <summary>
        /// Adds exception handling behaviors for commands, queries, and notifications.
        /// Exception handlers can be registered separately in the DI container.
        /// </summary>
        public static MediatorOptions AddExceptionHandlingBehaviors(this MediatorOptions options)
        {
            return options
                .AddOpenCommandPipelineBehavior(typeof(ExceptionHandlingCommandBehavior<,>))
                .AddOpenCommandPipelineBehavior(typeof(ExceptionHandlingVoidCommandBehavior<>))
                .AddOpenQueryPipelineBehavior(typeof(ExceptionHandlingQueryBehavior<,>))
                .AddOpenNotificationPipelineBehavior(typeof(ExceptionHandlingNotificationBehavior<>));
        }

        /// <summary>
        /// Adds exception handling behaviors for commands, queries, and notifications, and configures them.
        /// Exception handlers can be registered separately in the DI container.
        /// </summary>
        /// <param name="options">The mediator options.</param>
        /// <param name="configure">
        /// Configures <see cref="ExceptionHandlingOptions"/>, for example
        /// <c>o => o.SuppressNotificationExceptions = true</c>.
        /// </param>
        public static MediatorOptions AddExceptionHandlingBehaviors(
            this MediatorOptions options,
            System.Action<ExceptionHandlingOptions> configure)
        {
            if (configure == null)
                throw new System.ArgumentNullException(nameof(configure));

            options.ExceptionHandlingConfiguration += configure;
            return options.AddExceptionHandlingBehaviors();
        }

        /// <summary>
        /// Adds caching behavior for queries.
        /// Queries must implement ICacheableQuery or be decorated with [Cacheable] attribute.
        /// </summary>
        public static MediatorOptions AddCachingBehavior(this MediatorOptions options)
        {
            return options
                .AddOpenQueryPipelineBehavior(typeof(CachingQueryBehavior<,>));
        }

        /// <summary>
        /// Adds pre/post processor behaviors for commands, queries, and notifications.
        /// Pre-processors run before the handler, post-processors run after.
        /// Individual processors must be registered separately in the DI container.
        /// </summary>
        public static MediatorOptions AddProcessorBehaviors(this MediatorOptions options)
        {
            return options
                // Pre-processors should run first (outermost)
                .AddOpenCommandPipelineBehavior(typeof(RequestPreProcessorBehavior<,>))
                .AddOpenCommandPipelineBehavior(typeof(RequestPreProcessorBehavior<>))
                .AddOpenQueryPipelineBehavior(typeof(QueryPreProcessorBehavior<,>))
                .AddOpenNotificationPipelineBehavior(typeof(NotificationPreProcessorBehavior<>))
                // Post-processors should run last (innermost, closest to handler)
                .AddOpenCommandPipelineBehavior(typeof(RequestPostProcessorBehavior<,>))
                .AddOpenCommandPipelineBehavior(typeof(RequestPostProcessorBehavior<>))
                .AddOpenQueryPipelineBehavior(typeof(QueryPostProcessorBehavior<,>))
                .AddOpenNotificationPipelineBehavior(typeof(NotificationPostProcessorBehavior<>));
        }

        /// <summary>
        /// Adds both logging and exception handling behaviors.
        /// Exception handling behaviors are registered first so they wrap the logging behaviors.
        /// </summary>
        public static MediatorOptions AddDefaultBehaviorsWithExceptionHandling(this MediatorOptions options)
        {
            return options
                .AddExceptionHandlingBehaviors()
                .AddDefaultBehaviors();
        }

        /// <summary>
        /// Adds all default behaviors including logging, exception handling, caching, and processors.
        /// </summary>
        public static MediatorOptions AddAllBehaviors(this MediatorOptions options)
        {
            return options
                .AddProcessorBehaviors()
                .AddExceptionHandlingBehaviors()
                .AddCachingBehavior()
                .AddDefaultBehaviors();
        }
    }
}
