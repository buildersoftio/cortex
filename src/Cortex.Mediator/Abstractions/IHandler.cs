using System.Threading.Tasks;

namespace Cortex.Mediator
{
    public interface IHandler<TCommand, TResponse> where TCommand : ICommand
    {
        public Task<TResponse> Handle(TCommand command);
    }
}
