using Cortex.Core.Abstractions.Pipelines;
using Cortex.Core.Streams;

namespace Cortex.Core.Pipelines
{
    public interface IPipelineBuilder : IPipelineStreamBuilder
    {
        IPipelineStreamBuilder WithOptions();
    }
}
