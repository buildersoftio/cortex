using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Commands;
using FluentValidation;
using FluentValidation.Results;
using Moq;

namespace Cortex.Tests.Mediator.FluentValidation.Tests
{
    // Validator whose async rule records the cancellation token it receives.
    public class TokenCapturingValidator<T> : AbstractValidator<T>
    {
        public CancellationToken Seen { get; private set; }

        public TokenCapturingValidator()
        {
            RuleFor(x => x).MustAsync((x, ct) =>
            {
                Seen = ct;
                return Task.FromResult(true);
            });
        }
    }

    // Validator whose async rule stays pending until Gate is completed.
    public class PendingValidator<T> : AbstractValidator<T>
    {
        public TaskCompletionSource<bool> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PendingValidator()
        {
            RuleFor(x => x).MustAsync((x, ct) => Gate.Task);
        }
    }

    public class ValidationCommandBehaviorTests
    {
        public class FakeCommand : ICommand<string>
        {
        }

        [Fact]
        public async Task Handle_ShouldNotThrowExceptionsWhenThereAreNoValidationFailures()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeCommand>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default)).Returns(() => Task.FromResult(new ValidationResult { Errors = new List<ValidationFailure>()}));
            var validators = new IValidator<FakeCommand>[]
            {
                    validator.Object,
            };

            var next = new Mock<CommandHandlerDelegate<string>>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationCommandBehavior<FakeCommand, string>(validators);

            // Act

            var result = await systemUnderTest.Handle(new FakeCommand(), next.Object, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public async Task Handle_ShouldThrowAnExceptionsWhenThereIsAValidationFailure()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeCommand>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default))
                .Returns(() => Task.FromResult(new ValidationResult 
                { 
                    Errors = new List<ValidationFailure> 
                    { 
                        new ValidationFailure("some-property", "was invalid")
                    } 
                }
            ));

            var validators = new IValidator<FakeCommand>[]
            {
                    validator.Object,
            };

            var next = new Mock<CommandHandlerDelegate<string>>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationCommandBehavior<FakeCommand, string>(validators);

            // Act
            // Assert
            await Assert.ThrowsAsync<Cortex.Mediator.Exceptions.ValidationException>(async () => await systemUnderTest.Handle(new FakeCommand(), next.Object, CancellationToken.None));
        }

        [Fact]
        public async Task Handle_PassesCancellationTokenToValidators()
        {
            // Arrange
            var validator = new TokenCapturingValidator<FakeCommand>();
            var systemUnderTest = new ValidationCommandBehavior<FakeCommand, string>(new IValidator<FakeCommand>[] { validator });
            using var cts = new CancellationTokenSource();

            // Act
            await systemUnderTest.Handle(new FakeCommand(), () => Task.FromResult("completed"), cts.Token);

            // Assert
            Assert.Equal(cts.Token, validator.Seen);
        }

        [Fact]
        public async Task Handle_DoesNotBlockWhileAsyncValidatorIsPending()
        {
            // Arrange
            var validator = new PendingValidator<FakeCommand>();
            var systemUnderTest = new ValidationCommandBehavior<FakeCommand, string>(new IValidator<FakeCommand>[] { validator });
            Task<string>? handleTask = null;

            // Act: Handle must return a pending task instead of blocking the calling thread
            var call = Task.Run(() => { handleTask = systemUnderTest.Handle(new FakeCommand(), () => Task.FromResult("completed"), CancellationToken.None); });
            var returnedWhilePending = call.Wait(TimeSpan.FromSeconds(5));
            validator.Gate.SetResult(true);
            await call;

            // Assert
            Assert.True(returnedWhilePending);
            Assert.Equal("completed", await handleTask!);
        }
    }
}
