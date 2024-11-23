using Cortex.Streams.Abstractions;
using Cortex.Streams.Operators;
using System;

namespace Cortex.Streams.Extensions
{
    /// <summary>
    /// Extension methods for integrating custom operators into the StreamBuilder.
    /// </summary>
    public static class StreamBuilderExtensions
    {
        /// <summary>
        /// Adds a custom operator to the stream pipeline.
        /// </summary>
        /// <typeparam name="TIn">The type of the initial input to the stream.</typeparam>
        /// <typeparam name="TCurrent">The current type of data in the stream.</typeparam>
        /// <typeparam name="TNext">The resulting type after the operator.</typeparam>
        /// <param name="builder">The stream builder instance.</param>
        /// <param name="operatorInstance">The custom operator to add.</param>
        /// <returns>A new stream builder with the custom operator integrated.</returns>
        public static IStreamBuilder<TIn, TNext> UseOperator<TIn, TCurrent, TNext>(
            this IStreamBuilder<TIn, TCurrent> builder,
            IOperator operatorInstance)
            where TNext : class
        {
            if (builder == null)
                throw new ArgumentNullException(nameof(builder));

            if (operatorInstance == null)
                throw new ArgumentNullException(nameof(operatorInstance));

            // Integrate the custom operator into the pipeline

            if (builder == null)
                throw new ArgumentNullException(nameof(builder));

            if (operatorInstance == null)
                throw new ArgumentNullException(nameof(operatorInstance));

            // Integrate the custom operator into the pipeline
            builder.SetNext(operatorInstance);


            // Assuming the custom operator transforms TCurrent to TNext
            return (IStreamBuilder<TIn, TNext>)builder;
        }
    }
}
