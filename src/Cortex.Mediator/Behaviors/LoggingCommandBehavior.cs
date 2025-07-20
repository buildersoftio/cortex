using Cortex.Mediator.Commands;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior for logging command/query execution.
    /// </summary>
    public sealed class LoggingCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
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

            var stopwatch = Stopwatch.StartNew();   // start timing
            try
            {
                await next();

                stopwatch.Stop();
                _logger.LogInformation(
                    "Command {CommandName} executed successfully in {ElapsedMilliseconds} ms",
                    commandName,
                    stopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _logger.LogError(
                    ex,
                    "Error executing command {CommandName} after {ElapsedMilliseconds} ms",
                    commandName,
                    stopwatch.ElapsedMilliseconds);
                throw;
            }
        }
    }
}
