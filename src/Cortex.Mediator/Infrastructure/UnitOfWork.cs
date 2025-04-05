using System.Data;
using System.Threading.Tasks;

namespace Cortex.Mediator.Infrastructure
{
    /// <summary>
    /// Default implementation of IUnitOfWork using System.Data.
    /// </summary>
    public class UnitOfWork : IUnitOfWork
    {
        private readonly IDbConnection _connection;

        public UnitOfWork(IDbConnection connection)
        {
            _connection = connection;
        }

        public async Task<IUnitOfWorkTransaction> BeginTransactionAsync()
        {
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }

            var transaction = _connection.BeginTransaction();
            return new UnitOfWorkTransaction(transaction);
        }

        private class UnitOfWorkTransaction : IUnitOfWorkTransaction
        {
            private readonly IDbTransaction _transaction;
            private bool _disposed;

            public UnitOfWorkTransaction(IDbTransaction transaction)
            {
                _transaction = transaction;
            }

            public Task CommitAsync()
            {
                _transaction.Commit();
                return Task.CompletedTask;
            }

            public Task RollbackAsync()
            {
                _transaction.Rollback();
                return Task.CompletedTask;
            }

            public async ValueTask DisposeAsync()
            {
                if (_disposed) return;

                _transaction.Dispose();
                _disposed = true;
                await Task.CompletedTask;
            }
        }
    }
}
