using Cortex.Core.Designers;

namespace Cortex.Core.Sources
{
    public interface ISourceDesigner
    {
        // common non-generic methods and properties here
    }

    public interface ISourceDesigner<out TIn> : ISourceDesigner
    {
        IMapperDesigner<TOut> Map<TOut>(Func<TIn, TOut> func);
    }
}
