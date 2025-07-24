namespace Cortex.Mediator.Commands
{
    /// <summary>
    /// Represents a command in the CQRS pattern.
    /// Commands are used to change the system state and do return a value. 
    /// Please note that this is not a common practice in CQRS, as commands typically do not return values.
    /// </summary>
    public interface ICommand<TResult>
    {
    }
}
