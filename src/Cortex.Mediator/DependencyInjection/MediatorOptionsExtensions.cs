using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Commands;

namespace Cortex.Mediator.DependencyInjection
{
    public static class MediatorOptionsExtensions
    {
        public static MediatorOptions AddDefaultBehaviors(this MediatorOptions options)
        {
            return options
                .AddCommandPipelineBehavior<LoggingCommandBehavior<ICommand>>();
        }
    }
}
