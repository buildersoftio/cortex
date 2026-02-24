using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Processors;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
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
            Type[] handlerAssemblyMarkerTypes,
            Action<MediatorOptions> configure = null)
        {
            var options = new MediatorOptions();
            configure?.Invoke(options);

            services.AddScoped<IMediator, Mediator>();
            services.AddSingleton(typeof(INotificationPublishStrategy), options.NotificationPublishStrategyType);

            // Validation has been removed for issue #118
            //services.AddValidatorsFromAssemblies(handlerAssemblyMarkerTypes.Select(t => t.Assembly));

            RegisterHandlers(services, handlerAssemblyMarkerTypes, options);
            RegisterProcessors(services, handlerAssemblyMarkerTypes, options);
            RegisterPipelineBehaviors(services, options);

            return services;
        }

        private static void RegisterHandlers(
            IServiceCollection services,
            IEnumerable<Type> assemblyMarkerTypes,
            MediatorOptions options)
        {
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).ToArray();
            var lifetime = options.HandlerLifetime;

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(ICommandHandler<,>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithLifetime(lifetime));


            // feature #141 - Register void command handlers
            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(ICommandHandler<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithLifetime(lifetime));

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IQueryHandler<,>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithLifetime(lifetime));

            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(INotificationHandler<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithLifetime(lifetime));

            // Register streaming query handlers
            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IStreamQueryHandler<,>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithLifetime(lifetime));
        }

        private static void RegisterProcessors(
            IServiceCollection services,
            IEnumerable<Type> assemblyMarkerTypes,
            MediatorOptions options)
        {
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).ToArray();

            // Register pre-processors
            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IRequestPreProcessor<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithTransientLifetime());

            // Register post-processors with response
            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IRequestPostProcessor<,>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithTransientLifetime());

            // Register post-processors without response (for void commands)
            services.Scan(scan => scan
                .FromAssemblies(assemblies)
                .AddClasses(classes => classes
                    .AssignableTo(typeof(IRequestPostProcessor<>)), options.OnlyPublicClasses)
                .AsImplementedInterfaces()
                .WithTransientLifetime());
        }

        private static void RegisterPipelineBehaviors(IServiceCollection services, MediatorOptions options)
        {
            // Sort each behavior list by Order (stable sort preserves registration order for equal values).
            // OrderBy in LINQ is a stable sort.

            // Command behaviors
            foreach (var (behaviorType, _) in options.CommandBehaviors.OrderBy(b => b.Order))
            {
                services.AddTransient(typeof(ICommandPipelineBehavior<,>), behaviorType);
            }

            // feature #141 - Register non-returning command pipeline behaviors
            foreach (var (behaviorType, _) in options.VoidCommandBehaviors.OrderBy(b => b.Order))
            {
                services.AddTransient(typeof(ICommandPipelineBehavior<>), behaviorType);
            }

            // Query behaviors
            foreach (var (behaviorType, _) in options.QueryBehaviors.OrderBy(b => b.Order))
            {
                if (behaviorType.IsGenericTypeDefinition)
                {
                    // Open generic behavior - register against open generic interface
                    services.AddTransient(typeof(IQueryPipelineBehavior<,>), behaviorType);
                }
                else
                {
                    // Closed behavior - find and register against specific implemented interfaces
                    var implementedInterfaces = behaviorType.GetInterfaces()
                        .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryPipelineBehavior<,>));

                    foreach (var iface in implementedInterfaces)
                    {
                        services.AddTransient(iface, behaviorType);
                    }
                }
            }

            // Notification behaviors
            foreach (var (behaviorType, _) in options.NotificationBehaviors.OrderBy(b => b.Order))
            {
                if (behaviorType.IsGenericTypeDefinition)
                {
                    // Open generic behavior - register against open generic interface
                    services.AddTransient(typeof(INotificationPipelineBehavior<>), behaviorType);
                }
                else
                {
                    // Closed behavior - find and register against specific implemented interfaces
                    var implementedInterfaces = behaviorType.GetInterfaces()
                        .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INotificationPipelineBehavior<>));

                    foreach (var iface in implementedInterfaces)
                    {
                        services.AddTransient(iface, behaviorType);
                    }
                }
            }

            // Stream query behaviors
            foreach (var (behaviorType, _) in options.StreamQueryBehaviors.OrderBy(b => b.Order))
            {
                services.AddTransient(typeof(IStreamQueryPipelineBehavior<,>), behaviorType);
            }
        }
    }
}
