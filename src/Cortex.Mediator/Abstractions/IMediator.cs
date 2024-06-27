namespace Cortex.Mediator
{
    public interface IMediator
    {
        Task<TResponse> SendAsync<TCommand, TResponse>(TCommand command) where TCommand : ICommand;
    }
}
