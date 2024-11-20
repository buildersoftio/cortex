using Cortex.Streams.Operators;
using Cortex.Streams.RabbitMQ.Serializers;
using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Cortex.Streams.RabbitMQ
{ /// <summary>
  /// RabbitMQ Sink Operator with serialization support.
  /// </summary>
  /// <typeparam name="TInput">The type of objects to send.</typeparam>
    public class RabbitMQSinkOperator<TInput> : ISinkOperator<TInput>, IDisposable
    {
        private readonly string _hostname;
        private readonly string _queueName;
        private readonly string _username;
        private readonly string _password;
        private readonly ISerializer<TInput> _serializer;
        private IConnection _connection;
        private IModel _channel;
        private bool _isRunning;

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitMQSinkOperator{TInput}"/> class.
        /// </summary>
        /// <param name="hostname">The RabbitMQ server hostname.</param>
        /// <param name="queueName">The name of the RabbitMQ queue to publish to.</param>
        /// <param name="serializer">The serializer to convert TInput objects to strings.</param>
        /// <param name="username">The RabbitMQ username.</param>
        /// <param name="password">The RabbitMQ password.</param>
        public RabbitMQSinkOperator(string hostname, string queueName, string username = "guest", string password = "guest", ISerializer<TInput>? serializer = null)
        {
            _hostname = hostname ?? throw new ArgumentNullException(nameof(hostname));
            _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));

            _serializer = serializer ?? new DefaultJsonSerializer<TInput>();

            _username = username;
            _password = password;
            InitializeConnection();
        }

        private void InitializeConnection()
        {
            var factory = new ConnectionFactory()
            {
                HostName = _hostname,
                UserName = _username,
                Password = _password
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(queue: _queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);


            _isRunning = true;
        }

        /// <summary>
        /// Starts the sink operator.
        /// </summary>
        public void Start()
        {
            _isRunning = true;
        }

        /// <summary>
        /// Processes the input object by serializing it and sending it to the specified RabbitMQ queue.
        /// </summary>
        /// <param name="input">The input object to send.</param>
        public void Process(TInput input)
        {
            if (!_isRunning)
            {
                Console.WriteLine("RabbitMQSinkOperator is not running. Call Start() before processing messages.");
                return;
            }

            if (input == null)
            {
                Console.WriteLine("RabbitMQSinkOperator received null input. Skipping.");
                return;
            }

            Task.Run(() => SendMessageAsync(input));
        }

        /// <summary>
        /// Stops the sink operator.
        /// </summary>
        public void Stop()
        {
            _isRunning = false;
            Dispose();
            Console.WriteLine("RabbitMQSinkOperator stopped.");
        }

        /// <summary>
        /// Sends a serialized message to RabbitMQ asynchronously.
        /// </summary>
        /// <param name="obj">The input object to send.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task SendMessageAsync(TInput obj)
        {
            var serializedMessage = _serializer.Serialize(obj);
            var body = Encoding.UTF8.GetBytes(serializedMessage);

            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;

            try
            {
                _channel.BasicPublish(exchange: "",
                                     routingKey: _queueName,
                                     basicProperties: properties,
                                     body: body);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending message to RabbitMQ: {ex.Message}");
                // TODO: Implement retry logic or send to a dead-letter queue as needed.
            }

            await Task.CompletedTask;
        }

        /// <summary>
        /// Disposes the RabbitMQ connection and channel.
        /// </summary>
        public void Dispose()
        {
            _channel?.Close();
            _connection?.Close();
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }
}
