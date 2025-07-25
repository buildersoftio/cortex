﻿using Cortex.Mediator.Commands;
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
    public sealed class LoggingCommandBehavior<TCommand, TResult> : ICommandPipelineBehavior<TCommand, TResult>
        where TCommand : ICommand<TResult>
    {
        private readonly ILogger<LoggingCommandBehavior<TCommand, TResult>> _logger;

        public LoggingCommandBehavior(ILogger<LoggingCommandBehavior<TCommand, TResult>> logger)
        {
            _logger = logger;
        }

        public async Task<TResult> Handle(
            TCommand command,
            CommandHandlerDelegate<TResult> next,
            CancellationToken cancellationToken)
        {
            var commandName = typeof(TCommand).Name;
            _logger.LogInformation("Executing command {CommandName}", commandName);

            var stopwatch = Stopwatch.StartNew();   // start timing
            try
            {
                var result = await next();

                stopwatch.Stop();
                _logger.LogInformation(
                    "Command {CommandName} executed successfully in {ElapsedMilliseconds} ms",
                    commandName,
                    stopwatch.ElapsedMilliseconds);

                return result;
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
