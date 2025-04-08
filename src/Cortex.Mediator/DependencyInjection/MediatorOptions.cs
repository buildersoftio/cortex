using Cortex.Mediator.Commands;
using Cortex.Mediator.Queries;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cortex.Mediator.DependencyInjection
{
    public class MediatorOptions
    {
        internal List<Type> CommandBehaviors { get; } = new();
        internal List<Type> QueryBehaviors { get; } = new();

        public MediatorOptions AddCommandPipelineBehavior<TBehavior>()
            where TBehavior : ICommandPipelineBehavior<ICommand> // Add constraint
        {
            var behaviorType = typeof(TBehavior);
            if (behaviorType.IsGenericTypeDefinition)
            {
                throw new ArgumentException("Open generic types must be registered using AddOpenCommandPipelineBehavior");
            }
            CommandBehaviors.Add(behaviorType);
            return this;
        }

        public MediatorOptions AddOpenCommandPipelineBehavior(Type openGenericBehaviorType)
        {
            if (!openGenericBehaviorType.IsGenericTypeDefinition)
            {
                throw new ArgumentException("Type must be an open generic type definition");
            }

            CommandBehaviors.Add(openGenericBehaviorType);
            return this;
        }

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
    }
}
