using System;
using System.Collections.Generic;

namespace Cortex.Mediator.Exceptions
{
    /// <summary>
    /// Represents errors that occur during command/query validation.
    /// </summary>
    public class ValidationException : Exception
    {
        public ValidationException(IReadOnlyDictionary<string, string[]> errors)
            : base("Validation errors occurred")
        {
            Errors = errors;
        }

        public IReadOnlyDictionary<string, string[]> Errors { get; }
    }
}
