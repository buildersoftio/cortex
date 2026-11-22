using Cortex.Mediator;
using Cortex.Mediator.Commands;
using Cortex.Mediator.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace Cortex.Tests.Mediator.Tests
{
    #region Test Types for Mediator Registration Tests

    public class RegistrationTestCommand : ICommand<int>
    {
        public int Value { get; set; }
    }

    public class RegistrationTestCommandHandler : ICommandHandler<RegistrationTestCommand, int>
    {
        public Task<int> Handle(RegistrationTestCommand command, CancellationToken cancellationToken)
            => Task.FromResult(command.Value);
    }

    public class RegistrationOtherCommand : ICommand<int>
    {
        public int Value { get; set; }
    }

    public class RegistrationOtherCommandHandler : ICommandHandler<RegistrationOtherCommand, int>
    {
        public Task<int> Handle(RegistrationOtherCommand command, CancellationToken cancellationToken)
            => Task.FromResult(command.Value);
    }

    public class RegistrationTestVoidCommand : ICommand
    {
    }

    public class RegistrationTestVoidCommandHandler : ICommandHandler<RegistrationTestVoidCommand>
    {
        private readonly List<string> _log;

        public RegistrationTestVoidCommandHandler(List<string> log) => _log = log;

        public Task Handle(RegistrationTestVoidCommand command, CancellationToken cancellationToken)
        {
            _log.Add("Handler");
            return Task.CompletedTask;
        }
    }

    // Closed behavior: applies only to RegistrationTestCommand.
    public class AddHundredCommandBehavior : ICommandPipelineBehavior<RegistrationTestCommand, int>
    {
        public async Task<int> Handle(RegistrationTestCommand command, CommandHandlerDelegate<int> next, CancellationToken cancellationToken)
            => await next() + 100;
    }

    // Closed behavior for a void command.
    public class ClosedVoidCommandBehavior : ICommandPipelineBehavior<RegistrationTestVoidCommand>
    {
        private readonly List<string> _log;

        public ClosedVoidCommandBehavior(List<string> log) => _log = log;

        public async Task Handle(RegistrationTestVoidCommand command, CommandHandlerDelegate next, CancellationToken cancellationToken)
        {
            _log.Add("Behavior:Before");
            await next();
            _log.Add("Behavior:After");
        }
    }

    #endregion

    public class MediatorRegistrationTests
    {
        [Fact]
        public async Task ClosedCommandBehavior_IsRegisteredAndRunsOnlyForItsCommand()
        {
            var services = new ServiceCollection();
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddCommandPipelineBehavior<AddHundredCommandBehavior>();
            });
            services.AddTransient<ICommandHandler<RegistrationTestCommand, int>, RegistrationTestCommandHandler>();
            services.AddTransient<ICommandHandler<RegistrationOtherCommand, int>, RegistrationOtherCommandHandler>();

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            var result = await mediator.SendCommandAsync<RegistrationTestCommand, int>(new RegistrationTestCommand { Value = 1 });
            var otherResult = await mediator.SendCommandAsync<RegistrationOtherCommand, int>(new RegistrationOtherCommand { Value = 1 });

            Assert.Equal(101, result);
            Assert.Equal(1, otherResult);
        }

        [Fact]
        public async Task ClosedVoidCommandBehavior_IsRegisteredAndRuns()
        {
            var log = new List<string>();
            var services = new ServiceCollection();
            services.AddSingleton(log);
            services.AddCortexMediator(new Type[0], options =>
            {
                options.AddCommandPipelineBehavior<ClosedVoidCommandBehavior>();
            });
            services.AddTransient<ICommandHandler<RegistrationTestVoidCommand>, RegistrationTestVoidCommandHandler>();

            var provider = services.BuildServiceProvider();
            var mediator = provider.GetRequiredService<IMediator>();

            await mediator.SendCommandAsync(new RegistrationTestVoidCommand());

            Assert.Equal(new[] { "Behavior:Before", "Handler", "Behavior:After" }, log);
        }
    }
}
