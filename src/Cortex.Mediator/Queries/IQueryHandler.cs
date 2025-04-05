using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Queries
{
    /// <summary>
    /// Defines a handler for a query.
    /// </summary>
    /// <typeparam name="TQuery">The type of query being handled.</typeparam>
    /// <typeparam name="TResult">The type of result returned.</typeparam>
    public interface IQueryHandler<in TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        /// <summary>
        /// Handles the specified query.
        /// </summary>
        Task<TResult> Handle(TQuery query, CancellationToken cancellationToken);
    }

}
