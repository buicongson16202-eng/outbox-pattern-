namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Ranh giới với broker thật (RabbitMQ, Kafka, Azure Service Bus…).
    /// Hợp đồng: chỉ trả về thành công khi broker đã XÁC NHẬN đã nhận (publisher confirm / ack);
    /// mọi lỗi hoặc timeout phải ném exception để relay biết mà thử lại.
    /// </summary>
    public interface IMessagePublisher
    {
        ValueTask PublishAsync(BrokerMessage message, CancellationToken cancellationToken);
    }
}
