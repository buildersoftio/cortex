using Cortex.Streams.Operators;
using Cortex.Streams.RabbitMQ.Deserializers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Cortex.Streams.RabbitMQ
{
    /// <summary>
    /// RabbitMQ Source Operator with deserialization support.
    /// </summary>
    /// <typeparam name="TOutput">The type of objects to emit.</typeparam>
    public class RabbitMQSourceOperator<TOutput> : ISourceOperator<TOutput>, IDisposable
    {
        private readonly string _hostname;
        private readonly string _queueName;
        private readonly string _username;
        private readonly string _password;
        private readonly IDeserializer<TOutput> _deserializer;
        private IConnection _connection;
        private IModel _channel;
        private EventingBasicConsumer _consumer;
        private Action<TOutput> _emitAction;
        private bool _isRunning;

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitMQSourceOperator{TOutput}"/> class.
        /// </summary>
        /// <param name="hostname">The RabbitMQ server hostname.</param>
        /// <param name="queueName">The name of the RabbitMQ queue to consume from.</param>
        /// <param name="deserializer">The deserializer to convert message strings to TOutput objects.</param>
        /// <param name="username">The RabbitMQ username.</param>
        /// <param name="password">The RabbitMQ password.</param>
        public RabbitMQSourceOperator(string hostname, string queueName, string username = "guest", string password = "guest", IDeserializer<TOutput> deserializer = null)
        {
            _hostname = hostname ?? throw new ArgumentNullException(nameof(hostname));
            _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));

            _deserializer = deserializer ?? new DefaultJsonDeserializer<TOutput>();

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
                Password = _password,
                DispatchConsumersAsync = true // Enable asynchronous message handling
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(queue: _queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
        }

        /// <summary>
        /// Starts the source operator.
        /// </summary>
        /// <param name="emit">The action to emit deserialized objects.</param>
        public void Start(Action<TOutput> emit)
        {
            if (emit == null) throw new ArgumentNullException(nameof(emit));
            if (_isRunning) throw new InvalidOperationException("RabbitMQSourceOperator is already running.");

            _emitAction = emit;
            _consumer = new EventingBasicConsumer(_channel);
            _consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var obj = _deserializer.Deserialize(message);

                    _emitAction(obj);

                    // Acknowledge the message
                    _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message from RabbitMQ: {ex.Message}");
                    // Optionally reject and requeue the message or send to dead-letter queue
                    _channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            _channel.BasicConsume(queue: _queueName,
                                 autoAck: false, // Manual acknowledgment
                                 consumer: _consumer);

            _isRunning = true;
        }

        /// <summary>
        /// Stops the source operator.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;

            _channel.BasicCancel(_consumer.ConsumerTags[0]);
            Dispose();
            _isRunning = false;
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
