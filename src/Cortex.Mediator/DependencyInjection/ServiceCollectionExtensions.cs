using Cortex.Mediator.Commands;
using Cortex.Mediator.Infrastructure;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Queries;
using FluentValidation;
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
            services.AddValidatorsFromAssemblies(handlerAssemblyMarkerTypes.Select(t => t.Assembly));
            services.AddUnitOfWork();

            RegisterHandlers(services, handlerAssemblyMarkerTypes);
            RegisterPipelineBehaviors(services, options);

            return services;
        }

        private static void RegisterHandlers(
            IServiceCollection services,
            IEnumerable<Type> assemblyMarkerTypes)
        {
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).ToArray();

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(ICommandHandler<>)))
                .AsImplementedInterfaces()
                .WithScopedLifetime());

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IQueryHandler<,>)))
                .AsImplementedInterfaces()
                .WithScopedLifetime());

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(INotificationHandler<>)))
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
