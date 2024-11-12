namespace Cortex.Streams.Operators
{
    public interface IOperator
    {
        void Process(object input);
        void SetNext(IOperator nextOperator);
    }
}
