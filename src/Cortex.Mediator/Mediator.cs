namespace Cortex.Mediator
{
    public class Mediator : IMediator
    {
        private readonly IServiceProvider _serviceProvider;
        public Mediator(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public Task<TResponse> SendAsync<TCommand, TResponse>(TCommand command) where TCommand : ICommand
        {
            var handlerType = typeof(IHandler<TCommand, TResponse>);

            var handler = _serviceProvider.GetService(handlerType) as IHandler<TCommand, TResponse>;
            if (handler == null)
            {
                throw new InvalidOperationException($"No handler registered for {typeof(TCommand).Name}");
            }

            return handler.Handle(command);
        }
    }
}
