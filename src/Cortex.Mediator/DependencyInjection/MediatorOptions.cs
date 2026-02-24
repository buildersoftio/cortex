using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Mediator.DependencyInjection
{
    public class MediatorOptions
    {
        internal List<(Type BehaviorType, int Order)> CommandBehaviors { get; } = new();
        internal List<(Type BehaviorType, int Order)> VoidCommandBehaviors { get; } = new();
        internal List<(Type BehaviorType, int Order)> QueryBehaviors { get; } = new();
        internal List<(Type BehaviorType, int Order)> NotificationBehaviors { get; } = new();
        internal List<(Type BehaviorType, int Order)> StreamQueryBehaviors { get; } = new();

        public bool OnlyPublicClasses { get; set; } = true;

        /// <summary>
        /// Gets or sets the service lifetime for handler registrations
        /// (command handlers, query handlers, notification handlers, and stream query handlers).
        /// Defaults to <see cref="ServiceLifetime.Scoped"/>.
        /// </summary>
        public ServiceLifetime HandlerLifetime { get; set; } = ServiceLifetime.Scoped;

        /// <summary>
        /// Gets the type of notification publish strategy to use.
        /// Defaults to <see cref="ParallelNotificationStrategy"/>.
        /// Use <see cref="UseNotificationPublishStrategy{TStrategy}"/> to change.
        /// </summary>
        public Type NotificationPublishStrategyType { get; private set; } = typeof(ParallelNotificationStrategy);

        /// <summary>
        /// Sets the strategy used to publish notifications to multiple handlers.
        /// </summary>
        /// <typeparam name="TStrategy">
        /// The strategy implementation. Built-in options:
        /// <see cref="ParallelNotificationStrategy"/> (default) — all handlers run in parallel via Task.WhenAll,
        /// <see cref="SequentialNotificationStrategy"/> — handlers run one at a time in registration order,
        /// <see cref="StopOnFirstFailureNotificationStrategy"/> — sequential, stops on first exception.
        /// </typeparam>
        public MediatorOptions UseNotificationPublishStrategy<TStrategy>()
            where TStrategy : class, INotificationPublishStrategy
        {
            NotificationPublishStrategyType = typeof(TStrategy);
            return this;
        }


        /// <summary>
        /// Register a *closed* command pipeline behavior.
        /// </summary>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddCommandPipelineBehavior<TBehavior>(int order = 0)
            where TBehavior : class // Add constraint
        {
            var behaviorType = typeof(TBehavior);

            if (behaviorType.IsGenericTypeDefinition)
                throw new ArgumentException("Open generic types must be registered using AddOpenCommandPipelineBehavior");

            var implementsReturning =
                behaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                      i.GetGenericTypeDefinition() == typeof(ICommandPipelineBehavior<,>));

            var implementsNonReturning =
                behaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                      i.GetGenericTypeDefinition() == typeof(ICommandPipelineBehavior<>));

            if (!implementsReturning && !implementsNonReturning)
                throw new ArgumentException("Type must implement ICommandPipelineBehavior<,> or ICommandPipelineBehavior<>");

            if (implementsReturning)
                CommandBehaviors.Add((behaviorType, order));

            if (implementsNonReturning)
                VoidCommandBehaviors.Add((behaviorType, order));

            return this;
        }

        /// <summary>
        /// Register an *open generic* command pipeline behavior, e.g. typeof(LoggingCommandBehavior&lt;,&gt;).
        /// </summary>
        /// <param name="openGenericBehaviorType">The open generic behavior type.</param>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddOpenCommandPipelineBehavior(Type openGenericBehaviorType, int order = 0)
        {
            if (!openGenericBehaviorType.IsGenericTypeDefinition)
                throw new ArgumentException("Type must be an open generic type definition");

            var implementsReturning =
                openGenericBehaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                                 i.GetGenericTypeDefinition() == typeof(ICommandPipelineBehavior<,>));

            var implementsNonReturning =
                openGenericBehaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                                 i.GetGenericTypeDefinition() == typeof(ICommandPipelineBehavior<>));

            if (!implementsReturning && !implementsNonReturning)
                throw new ArgumentException("Type must implement ICommandPipelineBehavior<,> or ICommandPipelineBehavior<>");

            if (implementsReturning)
                CommandBehaviors.Add((openGenericBehaviorType, order));

            if (implementsNonReturning)
                VoidCommandBehaviors.Add((openGenericBehaviorType, order));

            return this;
        }

        /// <summary>
        /// Register a *closed* query pipeline behavior.
        /// </summary>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddQueryPipelineBehavior<TBehavior>(int order = 0)
            where TBehavior : class
        {
            var behaviorType = typeof(TBehavior);

            if (behaviorType.IsGenericTypeDefinition)
                throw new ArgumentException("Open generic types must be registered using AddOpenQueryPipelineBehavior");

            var implementsQueryBehavior =
                behaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                      i.GetGenericTypeDefinition() == typeof(IQueryPipelineBehavior<,>));

            if (!implementsQueryBehavior)
                throw new ArgumentException("Type must implement IQueryPipelineBehavior<,>");

            QueryBehaviors.Add((behaviorType, order));
            return this;
        }

        /// <summary>
        /// Register an *open generic* query pipeline behavior, e.g. typeof(CachingQueryBehavior&lt;,&gt;).
        /// </summary>
        /// <param name="openGenericBehaviorType">The open generic behavior type.</param>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddOpenQueryPipelineBehavior(Type openGenericBehaviorType, int order = 0)
        {
            if (!openGenericBehaviorType.IsGenericTypeDefinition)
            {
                throw new ArgumentException("Type must be an open generic type definition");
            }

            var queryBehaviorInterface = openGenericBehaviorType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType &&
                                   i.GetGenericTypeDefinition() == typeof(IQueryPipelineBehavior<,>));

            if (queryBehaviorInterface == null)
            {
                throw new ArgumentException("Type must implement IQueryPipelineBehavior<,>");
            }

            QueryBehaviors.Add((openGenericBehaviorType, order));
            return this;
        }

        /// <summary>
        /// Register a *closed* notification pipeline behavior.
        /// </summary>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddNotificationPipelineBehavior<TBehavior>(int order = 0)
            where TBehavior : class
        {
            var behaviorType = typeof(TBehavior);

            if (behaviorType.IsGenericTypeDefinition)
                throw new ArgumentException("Open generic types must be registered using AddOpenNotificationPipelineBehavior");

            var implementsNotificationBehavior =
                behaviorType.GetInterfaces().Any(i => i.IsGenericType &&
                                                      i.GetGenericTypeDefinition() == typeof(INotificationPipelineBehavior<>));

            if (!implementsNotificationBehavior)
                throw new ArgumentException("Type must implement INotificationPipelineBehavior<>");

            NotificationBehaviors.Add((behaviorType, order));
            return this;
        }

        /// <summary>
        /// Register an *open generic* notification pipeline behavior, e.g. typeof(LoggingNotificationBehavior&lt;&gt;).
        /// </summary>
        /// <param name="openGenericBehaviorType">The open generic behavior type.</param>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddOpenNotificationPipelineBehavior(Type openGenericBehaviorType, int order = 0)
        {
            if (!openGenericBehaviorType.IsGenericTypeDefinition)
            {
                throw new ArgumentException("Type must be an open generic type definition");
            }

            var notificationBehaviorInterface = openGenericBehaviorType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType &&
                                   i.GetGenericTypeDefinition() == typeof(INotificationPipelineBehavior<>));

            if (notificationBehaviorInterface == null)
            {
                throw new ArgumentException("Type must implement INotificationPipelineBehavior<>");
            }

            NotificationBehaviors.Add((openGenericBehaviorType, order));
            return this;
        }

        /// <summary>
        /// Register an *open generic* streaming query pipeline behavior, e.g. typeof(LoggingStreamQueryBehavior&lt;,&gt;).
        /// </summary>
        /// <param name="openGenericBehaviorType">The open generic behavior type.</param>
        /// <param name="order">
        /// Execution order. Lower values run first (outermost in the pipeline).
        /// Behaviors with the same order preserve registration order. Defaults to 0.
        /// </param>
        public MediatorOptions AddOpenStreamQueryPipelineBehavior(Type openGenericBehaviorType, int order = 0)
        {
            if (!openGenericBehaviorType.IsGenericTypeDefinition)
            {
                throw new ArgumentException("Type must be an open generic type definition");
            }

            var streamBehaviorInterface = openGenericBehaviorType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType &&
                                   i.GetGenericTypeDefinition() == typeof(IStreamQueryPipelineBehavior<,>));

            if (streamBehaviorInterface == null)
            {
                throw new ArgumentException("Type must implement IStreamQueryPipelineBehavior<,>");
            }

            StreamQueryBehaviors.Add((openGenericBehaviorType, order));
            return this;
        }
    }
}
