namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    /// <summary>
    /// A manually advanced clock for scheduling tests.
    /// </summary>
    public sealed class FakeTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        public FakeTimeProvider(DateTimeOffset start)
        {
            _now = start;
        }

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now = _now.Add(by);
    }
}
