using Cortex.Mediator.Commands;
using System;
using System.Text.Json;

namespace Cortex.Mediator.Scheduling.Serialization
{
    /// <summary>
    /// Serializes commands with System.Text.Json and stores their type as "Namespace.Type, AssemblyName",
    /// so stored commands survive assembly version changes.
    /// Only concrete types that implement <see cref="ICommand"/> are accepted when reading.
    /// </summary>
    public sealed class JsonScheduledCommandSerializer : IScheduledCommandSerializer
    {
        private readonly JsonSerializerOptions _options;

        /// <summary>Creates a serializer with the default System.Text.Json options.</summary>
        public JsonScheduledCommandSerializer()
            : this(new JsonSerializerOptions())
        {
        }

        /// <summary>Creates a serializer with custom System.Text.Json options.</summary>
        public JsonScheduledCommandSerializer(JsonSerializerOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc />
        public string GetTypeName(ICommand command)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));

            var type = command.GetType();
            return $"{type.FullName}, {type.Assembly.GetName().Name}";
        }

        /// <inheritdoc />
        public string Serialize(ICommand command)
        {
            if (command == null)
                throw new ArgumentNullException(nameof(command));

            return JsonSerializer.Serialize(command, command.GetType(), _options);
        }

        /// <inheritdoc />
        public ICommand Deserialize(string typeName, string payload)
        {
            if (string.IsNullOrWhiteSpace(typeName))
                throw new ArgumentException("The command type name must not be empty.", nameof(typeName));
            if (payload == null)
                throw new ArgumentNullException(nameof(payload));

            var type = Type.GetType(typeName, throwOnError: false);
            if (type == null)
                throw new InvalidOperationException($"Scheduled command type '{typeName}' could not be resolved.");

            if (!typeof(ICommand).IsAssignableFrom(type) || type.IsAbstract || type.IsInterface)
                throw new InvalidOperationException($"Scheduled command type '{typeName}' is not a concrete {nameof(ICommand)}.");

            var command = JsonSerializer.Deserialize(payload, type, _options);
            if (command == null)
                throw new InvalidOperationException($"Scheduled command payload for '{typeName}' deserialized to null.");

            return (ICommand)command;
        }
    }
}
