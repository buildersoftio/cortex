using System;
using System.Threading.Tasks;

namespace Cortex.Mediator.Infrastructure
{
    /// <summary>
    /// Represents a unit of work for transaction management.
    /// </summary>
    public interface IUnitOfWork
    {
        /// <summary>
        /// Begins a new transaction.
        /// </summary>
        Task<IUnitOfWorkTransaction> BeginTransactionAsync();
    }

    /// <summary>
    /// Represents a transaction within a unit of work.
    /// </summary>
    public interface IUnitOfWorkTransaction : IAsyncDisposable
    {
        /// <summary>
        /// Commits the transaction.
        /// </summary>
        Task CommitAsync();

        /// <summary>
        /// Rolls back the transaction.
        /// </summary>
        Task RollbackAsync();
    }
}
