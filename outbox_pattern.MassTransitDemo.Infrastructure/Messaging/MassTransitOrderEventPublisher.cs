using MassTransit;
using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Messaging
{
    /// <summary>
    /// Cài đặt cổng <see cref="IOrderEventPublisher"/> bằng bus outbox của MassTransit.
    /// Nhờ UseBusOutbox() (DependencyInjection.cs), IPublishEndpoint được resolve trong scope của request KHÔNG gửi lên broker:
    /// nó thêm một dòng OutboxMessage vào chính AppDbContext của scope đó, và dòng này chỉ được ghi xuống database khi
    /// IUnitOfWork.SaveChangesAsync chạy — cùng transaction với Order.
    /// </summary>
    public class MassTransitOrderEventPublisher(IPublishEndpoint publishEndpoint) : IOrderEventPublisher
    {
        public Task PublishOrderCreatedAsync(Order order, CancellationToken cancellationToken) =>
            publishEndpoint.Publish(
                new OrderCreated(order.Id, order.Sku, order.Quantity, order.CustomerEmail, order.CreatedOnUtc),
                cancellationToken);
    }
}
