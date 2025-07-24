using Cortex.Mediator.Behaviors;

namespace Cortex.Mediator.DependencyInjection
{
    public static class MediatorOptionsExtensions
    {
        public static MediatorOptions AddDefaultBehaviors(this MediatorOptions options)
        {
            return options
                // Register the open generic logging behavior for commands that return TResult
                .AddOpenCommandPipelineBehavior(typeof(LoggingCommandBehavior<,>))
                .AddOpenQueryPipelineBehavior(typeof(LoggingQueryBehavior<,>));
        }
    }
}
