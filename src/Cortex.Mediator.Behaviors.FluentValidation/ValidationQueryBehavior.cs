using Cortex.Mediator.Queries;
using FluentValidation;
using FluentValidation.Results;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors.FluentValidation
{
    public sealed class ValidationQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {

        private readonly IEnumerable<IValidator<TQuery>> _validators;

        public ValidationQueryBehavior(IEnumerable<IValidator<TQuery>> validators)
        {
            _validators = validators;
        }


        public async Task<TResult> Handle(TQuery query, QueryHandlerDelegate<TResult> next, CancellationToken cancellationToken)
        {
            if (!_validators.Any())
                return await next();

            var context = new ValidationContext<TQuery>(query);
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

            return await next();
        }
    }
}
