using Cortex.Mediator.Commands;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior for logging command/query execution.
    /// </summary>
    public class LoggingCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
        where TCommand : ICommand
    {
        private readonly ILogger<LoggingCommandBehavior<TCommand>> _logger;

        public LoggingCommandBehavior(ILogger<LoggingCommandBehavior<TCommand>> logger)
        {
            _logger = logger;
        }

        public async Task Handle(
            TCommand command,
            CommandHandlerDelegate next,
            CancellationToken cancellationToken)
        {
            var commandName = typeof(TCommand).Name;
            _logger.LogInformation("Executing command {CommandName}", commandName);

            try
            {
                await next();
                _logger.LogInformation("Command {CommandName} executed successfully", commandName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing command {CommandName}", commandName);
                throw;
            }
        }
    }
}
