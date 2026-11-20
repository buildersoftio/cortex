using Cortex.Mediator.Commands;
using Cortex.Mediator.Scheduling.Serialization;

namespace Cortex.Tests.Mediator.Scheduling.Tests
{
    public class JsonScheduledCommandSerializerTests
    {
        public class SendReminderCommand : ICommand
        {
            public string Email { get; set; } = string.Empty;
            public int Hours { get; set; }
        }

        public class NotACommand
        {
            public string Value { get; set; } = string.Empty;
        }

        public abstract class AbstractCommand : ICommand
        {
        }

        private readonly JsonScheduledCommandSerializer _serializer = new();

        [Fact]
        public void Serialize_ThenDeserialize_RoundTripsTheCommand()
        {
            var command = new SendReminderCommand { Email = "a@b.c", Hours = 24 };

            var typeName = _serializer.GetTypeName(command);
            var payload = _serializer.Serialize(command);
            var result = _serializer.Deserialize(typeName, payload);

            var typed = Assert.IsType<SendReminderCommand>(result);
            Assert.Equal("a@b.c", typed.Email);
            Assert.Equal(24, typed.Hours);
        }

        [Fact]
        public void GetTypeName_OmitsAssemblyVersion()
        {
            var typeName = _serializer.GetTypeName(new SendReminderCommand());

            Assert.Equal(
                $"{typeof(SendReminderCommand).FullName}, {typeof(SendReminderCommand).Assembly.GetName().Name}",
                typeName);
            Assert.DoesNotContain("Version=", typeName);
        }

        [Fact]
        public void Serialize_UsesTheRuntimeType()
        {
            ICommand command = new SendReminderCommand { Email = "x@y.z" };

            var payload = _serializer.Serialize(command);

            Assert.Contains("x@y.z", payload);
        }

        [Fact]
        public void Deserialize_TypeThatIsNotACommand_Throws()
        {
            var typeName = $"{typeof(NotACommand).FullName}, {typeof(NotACommand).Assembly.GetName().Name}";

            var ex = Assert.Throws<InvalidOperationException>(() => _serializer.Deserialize(typeName, "{}"));

            Assert.Contains("not a concrete ICommand", ex.Message);
        }

        [Fact]
        public void Deserialize_AbstractCommandType_Throws()
        {
            var typeName = $"{typeof(AbstractCommand).FullName}, {typeof(AbstractCommand).Assembly.GetName().Name}";

            Assert.Throws<InvalidOperationException>(() => _serializer.Deserialize(typeName, "{}"));
        }

        [Fact]
        public void Deserialize_UnknownType_Throws()
        {
            var ex = Assert.Throws<InvalidOperationException>(
                () => _serializer.Deserialize("Does.Not.Exist, Nowhere", "{}"));

            Assert.Contains("could not be resolved", ex.Message);
        }

        [Fact]
        public void Deserialize_EmptyTypeName_Throws()
        {
            Assert.Throws<ArgumentException>(() => _serializer.Deserialize(" ", "{}"));
        }

        [Fact]
        public void Serialize_NullCommand_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => _serializer.Serialize(null!));
            Assert.Throws<ArgumentNullException>(() => _serializer.GetTypeName(null!));
        }
    }
}
