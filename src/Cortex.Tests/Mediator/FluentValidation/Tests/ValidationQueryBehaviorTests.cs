using Cortex.Mediator.Behaviors.FluentValidation;
using Cortex.Mediator.Queries;
using FluentValidation;
using FluentValidation.Results;
using Moq;

namespace Cortex.Tests.Mediator.FluentValidation.Tests
{
    public class FakeQuery : IQuery<string>
    {
    }

    public class ValidationQueryBehaviorTests
    {
        [Fact]
        public async Task Handle_ShouldNotThrowExceptionsWhenThereAreNoValidationFailures()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeQuery>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default)).Returns(() => Task.FromResult(new ValidationResult { Errors = new List<ValidationFailure>()}));
            var validators = new IValidator<FakeQuery>[]
            {
                    validator.Object,
            };

            var next = new Mock<QueryHandlerDelegate<string>>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationQueryBehavior<FakeQuery, string>(validators);

            // Act
            var result = await systemUnderTest.Handle(new FakeQuery(), next.Object, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public async Task Handle_ShouldThrowAnExceptionsWhenThereIsAValidationFailure()
        {
            // Arrange
            var expectedResult = "completed";
            var validator = new Mock<IValidator<FakeQuery>>();
            validator.Setup(v => v.ValidateAsync(It.IsAny<IValidationContext>(), default))
                .Returns(() => Task.FromResult(new ValidationResult 
                {
                    Errors =
                    [
                        new("some-property", "was invalid")
                    ] 
                }
            ));

            var validators = new IValidator<FakeQuery>[]
            {
                    validator.Object,
            };

            var next = new Mock<QueryHandlerDelegate<string>>();
            next.Setup(n => n.Invoke()).Returns(Task.FromResult(expectedResult));
            var systemUnderTest = new ValidationQueryBehavior<FakeQuery, string>(validators);

            // Act
            // Assert
            await Assert.ThrowsAsync<Cortex.Mediator.Exceptions.ValidationException>(async () => await systemUnderTest.Handle(new FakeQuery(), next.Object, CancellationToken.None));
        }

        [Fact]
        public async Task Handle_PassesCancellationTokenToValidators()
        {
            // Arrange
            var validator = new TokenCapturingValidator<FakeQuery>();
            var systemUnderTest = new ValidationQueryBehavior<FakeQuery, string>(new IValidator<FakeQuery>[] { validator });
            using var cts = new CancellationTokenSource();

            // Act
            await systemUnderTest.Handle(new FakeQuery(), () => Task.FromResult("completed"), cts.Token);

            // Assert
            Assert.Equal(cts.Token, validator.Seen);
        }

        [Fact]
        public async Task Handle_DoesNotBlockWhileAsyncValidatorIsPending()
        {
            // Arrange
            var validator = new PendingValidator<FakeQuery>();
            var systemUnderTest = new ValidationQueryBehavior<FakeQuery, string>(new IValidator<FakeQuery>[] { validator });
            Task<string>? handleTask = null;

            // Act: Handle must return a pending task instead of blocking the calling thread
            var call = Task.Run(() => { handleTask = systemUnderTest.Handle(new FakeQuery(), () => Task.FromResult("completed"), CancellationToken.None); });
            var returnedWhilePending = call.Wait(TimeSpan.FromSeconds(5));
            validator.Gate.SetResult(true);
            await call;

            // Assert
            Assert.True(returnedWhilePending);
            Assert.Equal("completed", await handleTask!);
        }
    }
}
