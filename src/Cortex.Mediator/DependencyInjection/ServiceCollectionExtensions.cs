using Cortex.Mediator.Commands;
using Cortex.Mediator.Infrastructure;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Cortex.Mediator.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddCortexMediator(
            this IServiceCollection services,
            IConfiguration configuration,
            Type[] handlerAssemblyMarkerTypes,
            Action<MediatorOptions>? configure = null)
        {
            var options = new MediatorOptions();
            configure?.Invoke(options);

            services.AddScoped<IMediator, Mediator>();

            // Validation has been removed for issue #118
            //services.AddValidatorsFromAssemblies(handlerAssemblyMarkerTypes.Select(t => t.Assembly));

            services.AddUnitOfWork();

            RegisterHandlers(services, handlerAssemblyMarkerTypes, options);
            RegisterPipelineBehaviors(services, options);

            return services;
        }

        private static void RegisterHandlers(
            IServiceCollection services,
            IEnumerable<Type> assemblyMarkerTypes,
            MediatorOptions options)
        {
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).ToArray();

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(ICommandHandler<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithScopedLifetime());

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IQueryHandler<,>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithScopedLifetime());

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(INotificationHandler<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithScopedLifetime());
        }

        private static void RegisterPipelineBehaviors(IServiceCollection services, MediatorOptions options)
        {
            // Command behaviors
            foreach (var behaviorType in options.CommandBehaviors)
            {
                services.AddTransient(typeof(ICommandPipelineBehavior<>), behaviorType);
            }

            // Query behaviors (if needed)
            foreach (var behaviorType in options.QueryBehaviors)
            {
                services.AddTransient(typeof(IQueryPipelineBehavior<,>), behaviorType);
            }
        }

        private static void AddUnitOfWork(this IServiceCollection services)
        {
            services.AddScoped<IUnitOfWork>(provider =>
                new UnitOfWork(provider.GetRequiredService<IDbConnection>()));
        }
    }
}
