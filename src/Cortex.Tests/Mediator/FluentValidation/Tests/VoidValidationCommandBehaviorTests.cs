using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Commands;
using FluentValidation;
using FluentValidation.Results;
using Moq;

namespace Cortex.Tests.Mediator.FluentValidation.Tests
{
    public class FakeVoidCommand : ICommand
    {
    }

    public class VoidValidationCommandBehaviorTests
    {
        [Fact]
        public async Task Handle_ShouldNotThrowExceptionsWhenThereAreNoValidationFailures()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeVoidCommand>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default)).Returns(() => Task.FromResult(new ValidationResult { Errors = new List<ValidationFailure>()}));
            var validators = new IValidator<FakeVoidCommand>[]
            {
                    validator.Object,
            };

            var next = new Mock<CommandHandlerDelegate>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationCommandBehavior<FakeVoidCommand>(validators);

            // Act 
            // Assert
            await systemUnderTest.Handle(new FakeVoidCommand(), next.Object, CancellationToken.None);
        }

        [Fact]
        public async Task Handle_ShouldThrowAnExceptionsWhenThereIsAValidationFailure()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeVoidCommand>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default))
                .Returns(() => Task.FromResult(new ValidationResult 
                { 
                    Errors = new List<ValidationFailure> 
                    { 
                        new ValidationFailure("some-property", "was invalid")
                    } 
                }
            ));

            var validators = new IValidator<FakeVoidCommand>[]
            {
                    validator.Object,
            };

            var next = new Mock<CommandHandlerDelegate>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationCommandBehavior<FakeVoidCommand>(validators);

            // Act
            // Assert
            await Assert.ThrowsAsync<Cortex.Mediator.Exceptions.ValidationException>(async () => await systemUnderTest.Handle(new FakeVoidCommand(), next.Object, CancellationToken.None));
        }

        [Fact]
        public async Task Handle_PassesCancellationTokenToValidators()
        {
            // Arrange
            var validator = new TokenCapturingValidator<FakeVoidCommand>();
            var systemUnderTest = new ValidationCommandBehavior<FakeVoidCommand>(new IValidator<FakeVoidCommand>[] { validator });
            using var cts = new CancellationTokenSource();

            // Act
            await systemUnderTest.Handle(new FakeVoidCommand(), () => Task.CompletedTask, cts.Token);

            // Assert
            Assert.Equal(cts.Token, validator.Seen);
        }

        [Fact]
        public async Task Handle_DoesNotBlockWhileAsyncValidatorIsPending()
        {
            // Arrange
            var validator = new PendingValidator<FakeVoidCommand>();
            var systemUnderTest = new ValidationCommandBehavior<FakeVoidCommand>(new IValidator<FakeVoidCommand>[] { validator });
            var nextCalled = false;
            Task? handleTask = null;

            // Act: Handle must return a pending task instead of blocking the calling thread
            var call = Task.Run(() => { handleTask = systemUnderTest.Handle(new FakeVoidCommand(), () => { nextCalled = true; return Task.CompletedTask; }, CancellationToken.None); });
            var returnedWhilePending = call.Wait(TimeSpan.FromSeconds(5));
            validator.Gate.SetResult(true);
            await call;
            await handleTask!;

            // Assert
            Assert.True(returnedWhilePending);
            Assert.True(nextCalled);
        }
    }
}
