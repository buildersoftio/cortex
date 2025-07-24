using Cortex.Mediator.Queries;
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
    public sealed class LoggingQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        private readonly ILogger<LoggingQueryBehavior<TQuery, TResult>> _logger;

        public LoggingQueryBehavior(ILogger<LoggingQueryBehavior<TQuery, TResult>> logger)
        {
            _logger = logger;
        }

        public async Task<TResult> Handle(
            TQuery command,
            QueryHandlerDelegate<TResult> next,
            CancellationToken cancellationToken)
        {
            var queryName = typeof(TQuery).Name;
            _logger.LogInformation("Executing query {QueryName}", queryName);

            var stopwatch = Stopwatch.StartNew();   // start timing
            try
            {
                var result = await next();

                stopwatch.Stop();
                _logger.LogInformation(
                    "Query {QueryName} executed successfully in {ElapsedMilliseconds} ms",
                    queryName,
                    stopwatch.ElapsedMilliseconds);

                return result;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _logger.LogError(
                    ex,
                    "Error executing query {QueryName} after {ElapsedMilliseconds} ms",
                    queryName,
                    stopwatch.ElapsedMilliseconds);
                throw;
            }
        }
    }
}
