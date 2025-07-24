namespace Cortex.Mediator
{
    public readonly struct Unit
    {
        public static readonly Unit Value = new();
        public override string ToString() => "()";
    }
}
