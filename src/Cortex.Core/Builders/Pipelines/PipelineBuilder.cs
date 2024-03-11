using Cortex.Core.Abstractions.Pipelines;
using Cortex.Core.Streams;

namespace Cortex.Core.Pipelines
{
    public class PipelineBuilder : IPipelineBuilder
    {
        private readonly string _name;
        private PipelineBuilder(string name)
        {
            _name = name;
        }

        public static IPipelineBuilder CreateNewPipeline(string name)
        {
            return new PipelineBuilder(name);
        }




        public IPipelineStreamBuilder AddStream(string name, IStreamBuilder streamBuilder)
        {
            throw new NotImplementedException();
        }

        public IPipeline Build()
        {
            throw new NotImplementedException();
        }

        public IPipelineStreamBuilder WithOptions()
        {
            throw new NotImplementedException();
        }
    }
}
