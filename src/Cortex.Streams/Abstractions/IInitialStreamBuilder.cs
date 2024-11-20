using Cortex.Streams.Operators;
using Cortex.Telemetry;
using System;

namespace Cortex.Streams.Abstractions
{
    public interface IInitialStreamBuilder<TIn, TCurrent>
    {
        /// <summary>
        /// Start the stream inside the application, in-app streaming
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        IStreamBuilder<TIn, TCurrent> Stream();

        /// <summary>
        /// Start configuring the Stream
        /// </summary>
        /// <param name="sourceOperator">Type of the Source Operator</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        IStreamBuilder<TIn, TCurrent> Stream(ISourceOperator<TCurrent> sourceOperator);

        /// <summary>
        /// Configure Telemetry for the Stream
        /// </summary>
        /// <param name="telemetryProvider">Telemetry provider like OpenTelemetryProvider</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        IInitialStreamBuilder<TIn, TCurrent> WithTelemetry(ITelemetryProvider telemetryProvider);

        /// <summary>
        /// Adds a map operator to the branch to transform data.
        /// </summary>
        /// <typeparam name="TNext">The type of data after the transformation.</typeparam>
        /// <param name="mapFunction">A function to transform data.</param>
        /// <returns>The branch stream builder with the new data type.</returns>
        //IStreamBuilder<TIn, TNext> Map<TNext>(Func<TCurrent, TNext> mapFunction);

        /// <summary>
        /// Adds a filter operator to the branch.
        /// </summary>
        /// <param name="predicate">A function to filter data.</param>
        /// <returns>The branch stream builder for method chaining.</returns>
        //IStreamBuilder<TIn, TCurrent> Filter(Func<TCurrent, bool> predicate);
    }
}
