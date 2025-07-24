using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Commands
{
    /// <summary>
    /// Defines a pipeline behavior for wrapping command handlers.
    /// </summary>
    /// <typeparam name="TCommand">The type of command being handled.</typeparam>
    public interface ICommandPipelineBehavior<in TCommand, TResult>
        where TCommand : ICommand<TResult>
    {
        /// <summary>
        /// Handles the command and invokes the next behavior in the pipeline.
        /// </summary>
        Task<TResult> Handle(
            TCommand command,
            CommandHandlerDelegate<TResult> next,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// Represents a delegate that wraps the command handler execution.
    /// </summary>
    public delegate Task<TResult> CommandHandlerDelegate<TResult>();
}
