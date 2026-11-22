using Cortex.Mediator.Commands;
using FluentValidation;
using FluentValidation.Results;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior for validation command execution.
    /// </summary>
    public sealed class ValidationCommandBehavior<TCommand> : ICommandPipelineBehavior<TCommand>
        where TCommand : ICommand
    {
        private readonly IEnumerable<IValidator<TCommand>> _validators;

        public ValidationCommandBehavior(IEnumerable<IValidator<TCommand>> validators)
        {
            _validators = validators;
        }


        public async Task Handle(TCommand command, CommandHandlerDelegate next, CancellationToken cancellationToken)
        {
            if (!_validators.Any())
            {
                await next();
                return;
            }

            var context = new ValidationContext<TCommand>(command);
            var failures = new List<ValidationFailure>();
            foreach (var validator in _validators)
            {
                var result = await validator.ValidateAsync(context, cancellationToken);
                failures.AddRange(result.Errors.Where(f => f != null));
            }

            if (failures.Count > 0)
            {
                var errors = failures
                    .GroupBy(f => f.PropertyName)
                    .ToDictionary(
                        g => g.Key,
                        g => g.Select(f => f.ErrorMessage).ToArray());

                throw new Exceptions.ValidationException(errors);
            }

            await next();
        }
    }
}
