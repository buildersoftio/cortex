namespace Cortex.Core.Pipelines
{
    public interface IPipeline
    {
        void Run();
        void RunOnes();
        void RunAsync();
        void RunOnesAsync();
    }
}
