using Cortex.Mediator.Commands;
using Cortex.Mediator.Infrastructure;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior for wrapping command execution in a transaction.
    /// </summary>
    public class TransactionCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
        where TCommand : ICommand
    {
        private readonly IUnitOfWork _unitOfWork;

        public TransactionCommandBehavior(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task Handle(
            TCommand command,
            CommandHandlerDelegate next,
            CancellationToken cancellationToken)
        {
            await using var transaction = await _unitOfWork.BeginTransactionAsync();
            try
            {
                await next();
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
    }
}
