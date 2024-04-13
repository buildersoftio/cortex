namespace Cortex.Core.Designers
{
    public interface IMapperDesigner
    {
        // common non-generic methods and properties here
    }

    public interface IMapperDesigner<TResult> : IMapperDesigner
    {
        IMapperDesigner<TOut> Map<TOut>(Func<TResult, TOut> func);
        ISinkDesigner<TResult> Sink(Action<TResult> action);
    }
}
