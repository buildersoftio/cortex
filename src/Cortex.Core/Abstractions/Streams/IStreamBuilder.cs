using Cortex.Core.Sources;

namespace Cortex.Core.Streams
{
    public interface IStreamBuilder
    {
        ISourceDesigner<T> From<T>();
    }
}
