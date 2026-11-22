namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Options for the exception handling behaviors.
    /// Set them with <c>options.AddExceptionHandlingBehaviors(o => ...)</c> or
    /// <c>services.Configure&lt;ExceptionHandlingOptions&gt;(o => ...)</c>.
    /// </summary>
    public class ExceptionHandlingOptions
    {
        /// <summary>
        /// Gets or sets whether <see cref="ExceptionHandlingNotificationBehavior{TNotification}"/> logs a notification
        /// handler's exception and swallows it instead of rethrowing it. Defaults to <c>false</c>.
        /// </summary>
        /// <remarks>
        /// The behavior wraps each handler separately, so with a sequential publish strategy the remaining handlers
        /// still run, and with parallel publishing the publish call completes without an exception.
        /// </remarks>
        public bool SuppressNotificationExceptions { get; set; }
    }
}
