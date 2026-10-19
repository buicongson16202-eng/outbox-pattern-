using System.Threading.Channels;

namespace outbox_pattern.Messaging
{
    public enum BrokerMode
    {
        Up,

        /// <summary>Broker sập: publish ném lỗi và message KHÔNG được nhận.</summary>
        Down,

        /// <summary>
        /// Broker đã nhận message nhưng ack bị mất (timeout, đứt mạng…): publisher thấy lỗi dù message đã được giao.
        /// Đây là nguồn gốc của bản trùng trong cơ chế at-least-once.
        /// </summary>
        LoseAck,
    }

    /// <summary>
    /// Broker giả lập trong bộ nhớ, thay cho RabbitMQ/Kafka để demo chạy được mà không cần hạ tầng ngoài.
    /// Khác broker thật ở hai điểm: (1) mất sạch khi process tắt; (2) mỗi process có một broker riêng, nên khi
    /// chạy nhiều instance thì message do instance nào publish sẽ do consumer của chính instance đó nhận.
    /// </summary>
    public class InMemoryBroker(ILogger<InMemoryBroker> logger) : IMessagePublisher
    {
        private readonly Channel<BrokerMessage> _queue = Channel.CreateUnbounded<BrokerMessage>();
        private volatile BrokerMode _mode = BrokerMode.Up;

        public BrokerMode Mode
        {
            get => _mode;
            set => _mode = value;
        }

        public ValueTask PublishAsync(BrokerMessage message, CancellationToken cancellationToken)
        {
            switch (Mode)
            {
                case BrokerMode.Down:
                    throw new InvalidOperationException("Broker is down (simulated)");

                case BrokerMode.LoseAck:
                    Enqueue(message);
                    throw new TimeoutException("Broker accepted the message but the ack was lost (simulated)");

                default:
                    Enqueue(message);
                    return ValueTask.CompletedTask;
            }
        }

        public IAsyncEnumerable<BrokerMessage> ReadAllAsync(CancellationToken cancellationToken) =>
            _queue.Reader.ReadAllAsync(cancellationToken);

        /// <summary>Mô phỏng broker giao lại message mà consumer chưa ack (nack/requeue).</summary>
        public ValueTask RequeueAsync(BrokerMessage message, CancellationToken cancellationToken) =>
            _queue.Writer.WriteAsync(message, cancellationToken);

        private void Enqueue(BrokerMessage message)
        {
            _queue.Writer.TryWrite(message);
            logger.LogInformation(
                "Broker accepted message {MessageId} ({Type}, key {Key})", message.Id, message.Type, message.Key);
        }
    }
}
