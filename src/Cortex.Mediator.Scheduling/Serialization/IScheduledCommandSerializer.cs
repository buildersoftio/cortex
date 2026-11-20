using Cortex.Mediator.Commands;

namespace Cortex.Mediator.Scheduling.Serialization
{
    /// <summary>
    /// Converts commands to and from the text stored with a scheduled command.
    /// </summary>
    public interface IScheduledCommandSerializer
    {
        /// <summary>Returns the type name to store for the command's runtime type.</summary>
        string GetTypeName(ICommand command);

        /// <summary>Serializes the command using its runtime type.</summary>
        string Serialize(ICommand command);

        /// <summary>
        /// Recreates a command from a stored type name and payload.
        /// Throws <see cref="System.InvalidOperationException"/> if the type cannot be resolved
        /// or is not a concrete <see cref="ICommand"/>.
        /// </summary>
        ICommand Deserialize(string typeName, string payload);
    }
}
