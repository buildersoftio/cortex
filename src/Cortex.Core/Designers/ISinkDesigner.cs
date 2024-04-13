using Cortex.Core.Streams;

namespace Cortex.Core.Designers
{
    public interface ISinkDesigner
    {
        // common non-generic methods and properties here
    }

    public interface ISinkDesigner<in TResult> : ISinkDesigner  
    {
        IStream Build();
    }
}
