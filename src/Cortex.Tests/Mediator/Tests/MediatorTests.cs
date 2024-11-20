using Moq;

namespace Cortex.Mediator.Tests
{
    public class MediatorTests
    {
        private readonly Mock<IServiceProvider> _mockServiceProvider;
        private readonly Cortex.Mediator.Mediator _mediator;

        public MediatorTests()
        {
            _mockServiceProvider = new Mock<IServiceProvider>();
            _mediator = new Mediator(_mockServiceProvider.Object);
        }

        [Fact]
        public async Task SendAsync_ShouldInvokeHandler()
        {
            // Arrange
            var mockHandler = new Mock<IHandler<ICommand, string>>();
            _mockServiceProvider
                .Setup(sp => sp.GetService(typeof(IHandler<ICommand, string>)))
                .Returns(mockHandler.Object);

            var command = new Mock<ICommand>();
            var expectedResponse = "Response";
            mockHandler.Setup(h => h.Handle(command.Object)).ReturnsAsync(expectedResponse);

            // Act
            var result = await _mediator.SendAsync<ICommand, string>(command.Object);

            // Assert
            Assert.Equal(expectedResponse, result);
            mockHandler.Verify(h => h.Handle(command.Object), Times.Once);
        }

        [Fact]
        public async Task SendAsync_ShouldThrowIfHandlerNotFound()
        {
            // Arrange
            _mockServiceProvider
                .Setup(sp => sp.GetService(It.IsAny<Type>()))
                .Returns(null);

            var command = new Mock<ICommand>();

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => _mediator.SendAsync<ICommand, string>(command.Object));
        }
    }
}
