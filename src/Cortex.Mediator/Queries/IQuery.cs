namespace Cortex.Mediator.Queries
{
    /// <summary>
    /// Represents a query in the CQRS pattern.
    /// Queries are used to read data and return a result.
    /// </summary>
    /// <typeparam name="TResult">The type of result returned by the query.</typeparam>
    public interface IQuery<out TResult>
    {
    }
}
