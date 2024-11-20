namespace Cortex.Telemetry
{
    public interface IMetricsProvider
    {
        ICounter CreateCounter(string name, string description = null);
        IHistogram CreateHistogram(string name, string description = null);
    }

    public interface ICounter
    {
        void Increment(double value = 1);
    }

    public interface IHistogram
    {
        void Record(double value);
    }
}
