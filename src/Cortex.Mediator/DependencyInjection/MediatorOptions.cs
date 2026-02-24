using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Mediator.DependencyInjection
{
    public class MediatorOptions
    {
        internal List<Type> CommandBehaviors { get; } = new();
        internal List<Type> VoidCommandBehaviors { get; } = new();
        internal List<Type> QueryBehaviors { get; } = new();
        internal List<Type> NotificationBehaviors { get; } = new();
        internal List<Type> StreamQueryBehaviors { get; } = new();

        public bool OnlyPublicClasses { get; set; } = true;


        /// <summary>
        /// Register a *closed* command pipeline behavior.
        /// </summary>
        public MediatorOptions AddCommandPipelineBehavior<TBehavior>()
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
                CommandBehaviors.Add(behaviorType);

            if (implementsNonReturning)
                VoidCommandBehaviors.Add(behaviorType);

            return this;
        }

        /// <summary>
        /// Register an *open generic* command pipeline behavior, e.g. typeof(LoggingCommandBehavior&lt;,&gt;).
        /// </summary>
        public MediatorOptions AddOpenCommandPipelineBehavior(Type openGenericBehaviorType)
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
                CommandBehaviors.Add(openGenericBehaviorType);

            if (implementsNonReturning)
                VoidCommandBehaviors.Add(openGenericBehaviorType);

            return this;
        }

        /// <summary>
        /// Register a *closed* query pipeline behavior.
        /// </summary>
        public MediatorOptions AddQueryPipelineBehavior<TBehavior>()
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

            QueryBehaviors.Add(behaviorType);
            return this;
        }

        /// <summary>
        /// Register an *open generic* query pipeline behavior, e.g. typeof(CachingQueryBehavior&lt;,&gt;).
        /// </summary>
        public MediatorOptions AddOpenQueryPipelineBehavior(Type openGenericBehaviorType)
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

            QueryBehaviors.Add(openGenericBehaviorType);
            return this;
        }

        /// <summary>
        /// Register a *closed* notification pipeline behavior.
        /// </summary>
        public MediatorOptions AddNotificationPipelineBehavior<TBehavior>()
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

            NotificationBehaviors.Add(behaviorType);
            return this;
        }

        /// <summary>
        /// Register an *open generic* notification pipeline behavior, e.g. typeof(LoggingNotificationBehavior&lt;&gt;).
        /// </summary>
        public MediatorOptions AddOpenNotificationPipelineBehavior(Type openGenericBehaviorType)
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

            NotificationBehaviors.Add(openGenericBehaviorType);
            return this;
        }

        /// <summary>
        /// Register an *open generic* streaming query pipeline behavior, e.g. typeof(LoggingStreamQueryBehavior&lt;,&gt;).
        /// </summary>
        public MediatorOptions AddOpenStreamQueryPipelineBehavior(Type openGenericBehaviorType)
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

            StreamQueryBehaviors.Add(openGenericBehaviorType);
            return this;
        }
    }
}
