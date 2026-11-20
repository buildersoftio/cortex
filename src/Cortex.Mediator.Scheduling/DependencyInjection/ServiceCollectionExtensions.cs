using Cortex.Mediator.Scheduling.Serialization;
using Cortex.Mediator.Scheduling.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace Cortex.Mediator.Scheduling.DependencyInjection
{
    /// <summary>
    /// Extension methods for registering scheduled command execution.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds <see cref="ICommandScheduler"/>, the in-memory store (unless a store is already registered),
        /// the JSON serializer and <see cref="ScheduledCommandProcessor"/>, registered as a hosted service
        /// unless <see cref="SchedulingOptions.EnableProcessor"/> is <c>false</c>.
        /// Requires Cortex.Mediator to be registered with <c>AddCortexMediator</c>.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configure">Optional configuration of <see cref="SchedulingOptions"/>.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddCortexMediatorScheduling(
            this IServiceCollection services,
            Action<SchedulingOptions> configure = null)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            var options = new SchedulingOptions();
            configure?.Invoke(options);
            options.Validate();

            services.TryAddSingleton(options);
            services.TryAddSingleton(TimeProvider.System);
            services.TryAddSingleton<IScheduledCommandSerializer>(_ => new JsonScheduledCommandSerializer());
            services.TryAddSingleton<IScheduledCommandStore, InMemoryScheduledCommandStore>();
            services.TryAddSingleton<ICommandScheduler, CommandScheduler>();
            services.TryAddSingleton<ScheduledCommandProcessor>();

            if (options.EnableProcessor)
                services.AddHostedService(sp => sp.GetRequiredService<ScheduledCommandProcessor>());

            return services;
        }
    }
}
