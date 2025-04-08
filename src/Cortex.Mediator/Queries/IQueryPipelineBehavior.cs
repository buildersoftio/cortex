using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Queries
{
    /// <summary>
    /// Defines a pipeline behavior for wrapping query handlers.
    /// </summary>
    /// <typeparam name="TQuery">The type of query being handled.</typeparam>
    /// <typeparam name="TResult">The type of result returned.</typeparam>
    public interface IQueryPipelineBehavior<in TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        /// <summary>
        /// Handles the query and invokes the next behavior in the pipeline.
        /// </summary>
        Task<TResult> Handle(
            TQuery query,
            QueryHandlerDelegate<TResult> next,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// Represents a delegate that wraps the query handler execution.
    /// </summary>
    public delegate Task<TResult> QueryHandlerDelegate<TResult>();

}
