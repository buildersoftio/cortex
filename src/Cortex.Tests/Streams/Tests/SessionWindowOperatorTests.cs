using Cortex.States;
using Cortex.Streams.Operators;

namespace Cortex.Streams.Tests
{
    public class SessionWindowOperatorTests
    {
        [Fact]
        public void Process_ShouldStartNewSession()
        {
            // Arrange
            var stateStore = new InMemoryStateStore<string, SessionWindowState<int>>("SessionTestStore");
            var sessionOperator = new SessionWindowOperator<int, string, string>(
                x => x.ToString(),
                TimeSpan.FromSeconds(10),
                inputs => string.Join(",", inputs),
                stateStore
            );

            // Act
            sessionOperator.Process(2);
            sessionOperator.Process(1);
            sessionOperator.Process(2);

            // Assert
            var session = stateStore.Get("2");
            Assert.NotNull(session);
            Assert.Equal(2, session.Events.Count);
        }

        [Fact]
        public void CheckForInactiveSessions_ShouldCloseExpiredSessions()
        {
            // Arrange
            var stateStore = new InMemoryStateStore<string, SessionWindowState<int>>("SessionTestStore");
            var sessionOperator = new SessionWindowOperator<int, string, string>(
                x => x.ToString(),
                TimeSpan.FromMilliseconds(500),
                inputs => string.Join(",", inputs),
                stateStore
            );

            sessionOperator.Process(1);

            // Wait for inactivity gap
            Thread.Sleep(100);
            Thread.Sleep(100);
            Thread.Sleep(100);
            Thread.Sleep(100);
            Thread.Sleep(100);
            Thread.Sleep(100);

            // Act
            // acting should be done automatically by the framework
            //sessionOperator.CheckForInactiveSessions();

            // Assert
            var session = stateStore.Get("1");
            Assert.Null(session);
        }
    }
}
