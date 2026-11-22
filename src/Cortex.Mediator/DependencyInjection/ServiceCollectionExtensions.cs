using Cortex.Mediator.Behaviors;
using Cortex.Mediator.Commands;
using Cortex.Mediator.Notifications;
using Cortex.Mediator.Processors;
using Cortex.Mediator.Queries;
using Cortex.Mediator.Streaming;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).Distinct().ToArray();
            var lifetime = options.HandlerLifetime;

            ScanAndRegister(services, assemblies, typeof(ICommandHandler<,>), options.OnlyPublicClasses, lifetime);

            // feature #141 - Register void command handlers
            ScanAndRegister(services, assemblies, typeof(ICommandHandler<>), options.OnlyPublicClasses, lifetime);

            ScanAndRegister(services, assemblies, typeof(IQueryHandler<,>), options.OnlyPublicClasses, lifetime);

            ScanAndRegister(services, assemblies, typeof(INotificationHandler<>), options.OnlyPublicClasses, lifetime);

            // Register streaming query handlers
            ScanAndRegister(services, assemblies, typeof(IStreamQueryHandler<,>), options.OnlyPublicClasses, lifetime);
        }

        private static void RegisterProcessors(
            IServiceCollection services,
            IEnumerable<Type> assemblyMarkerTypes,
            MediatorOptions options)
        {
            var assemblies = assemblyMarkerTypes.Select(t => t.Assembly).Distinct().ToArray();

            // Register pre-processors
            ScanAndRegister(services, assemblies, typeof(IRequestPreProcessor<>), options.OnlyPublicClasses, ServiceLifetime.Transient);

            // Register post-processors with response
            ScanAndRegister(services, assemblies, typeof(IRequestPostProcessor<,>), options.OnlyPublicClasses, ServiceLifetime.Transient);

            // Register post-processors without response (for void commands)
            ScanAndRegister(services, assemblies, typeof(IRequestPostProcessor<>), options.OnlyPublicClasses, ServiceLifetime.Transient);
        }

        private static void ScanAndRegister(
            IServiceCollection services,
            IEnumerable<Assembly> assemblies,
            Type openGenericInterface,
            bool onlyPublicClasses,
            ServiceLifetime lifetime)
        {
            foreach (var assembly in assemblies)
            {
                Type[] types;
                try
                {
                    types = onlyPublicClasses
                        ? assembly.GetExportedTypes()
                        : assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    types = ex.Types.Where(t => t != null).ToArray();
                }

                foreach (var type in types)
                {
                    if (type.IsInterface || type.IsAbstract || type.IsGenericTypeDefinition)
                        continue;

                    var matchingInterfaces = type.GetInterfaces()
                        .Where(i => i.IsGenericType &&
                                    i.GetGenericTypeDefinition() == openGenericInterface);

                    // TryAddEnumerable: a type found again (another marker in the same assembly,
                    // or a second AddCortexMediator call) is not registered twice.
                    foreach (var serviceType in matchingInterfaces)
                    {
                        services.TryAddEnumerable(new ServiceDescriptor(serviceType, type, lifetime));
                    }
                }
            }
        }

        public static void RegisterPipelineBehaviors(IServiceCollection services, MediatorOptions options)
        {
            // Options given to AddExceptionHandlingBehaviors(configure); the behaviors read them through IOptions.
            if (options.ExceptionHandlingConfiguration != null)
            {
                services.Configure<ExceptionHandlingOptions>(options.ExceptionHandlingConfiguration);
            }

            // Sort each behavior list by Order (stable sort preserves registration order for equal values).
            // OrderBy in LINQ is a stable sort.

            // Command behaviors
            foreach (var (behaviorType, _) in options.CommandBehaviors.OrderBy(b => b.Order))
            {
                RegisterBehavior(services, typeof(ICommandPipelineBehavior<,>), behaviorType);
            }

            // feature #141 - Register non-returning command pipeline behaviors
            foreach (var (behaviorType, _) in options.VoidCommandBehaviors.OrderBy(b => b.Order))
            {
                RegisterBehavior(services, typeof(ICommandPipelineBehavior<>), behaviorType);
            }

            // Query behaviors
            foreach (var (behaviorType, _) in options.QueryBehaviors.OrderBy(b => b.Order))
            {
                RegisterBehavior(services, typeof(IQueryPipelineBehavior<,>), behaviorType);
            }

            // Notification behaviors
            foreach (var (behaviorType, _) in options.NotificationBehaviors.OrderBy(b => b.Order))
            {
                RegisterBehavior(services, typeof(INotificationPipelineBehavior<>), behaviorType);
            }

            // Stream query behaviors
            foreach (var (behaviorType, _) in options.StreamQueryBehaviors.OrderBy(b => b.Order))
            {
                RegisterBehavior(services, typeof(IStreamQueryPipelineBehavior<,>), behaviorType);
            }
        }

        /// <summary>
        /// Registers an open generic behavior against the open generic interface, and a closed behavior
        /// against each closed form of that interface it implements. A behavior that is already registered
        /// for a service type is not registered again, so it never runs twice in one pipeline.
        /// </summary>
        private static void RegisterBehavior(IServiceCollection services, Type openGenericInterface, Type behaviorType)
        {
            if (behaviorType.IsGenericTypeDefinition)
            {
                services.TryAddEnumerable(ServiceDescriptor.Transient(openGenericInterface, behaviorType));
                return;
            }

            var implementedInterfaces = behaviorType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == openGenericInterface);

            foreach (var iface in implementedInterfaces)
            {
                services.TryAddEnumerable(ServiceDescriptor.Transient(iface, behaviorType));
            }
        }
    }
}
