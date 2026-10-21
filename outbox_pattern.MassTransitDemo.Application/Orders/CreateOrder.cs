using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Application.Orders
{
    public record CreateOrderCommand(string Sku, int Quantity, string CustomerEmail);

    /// <summary>
    /// Use case "tạo order". Đây là chỗ outbox pattern hiện ra trong Application: thêm Order, "publish" OrderCreated,
    /// rồi commit MỘT lần — Order và message cùng sống hoặc cùng chết. Class này không biết message đi qua đâu.
    /// </summary>
    public class CreateOrderHandler(
        IOrderRepository orders,
        IOrderEventPublisher events,
        IUnitOfWork unitOfWork,
        TimeProvider clock)
    {
        public async Task<OrderDto> HandleAsync(CreateOrderCommand command, CancellationToken cancellationToken)
        {
            var order = Order.Create(command.Sku, command.Quantity, command.CustomerEmail, clock.GetUtcNow().UtcDateTime);

            orders.Add(order);
            await events.PublishOrderCreatedAsync(order, cancellationToken);

            // Một transaction duy nhất cho cả order lẫn message (xem OUTBOX_PATTERN.md mục 4.1).
            await unitOfWork.SaveChangesAsync(cancellationToken);

            return OrderDto.From(order);
        }
    }

    public class GetOrderHandler(IOrderRepository orders)
    {
        public async Task<OrderDto?> HandleAsync(Guid id, CancellationToken cancellationToken)
        {
            var order = await orders.GetByIdAsync(id, cancellationToken);

            return order is null ? null : OrderDto.From(order);
        }
    }
}
