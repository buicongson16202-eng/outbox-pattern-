using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Application.Orders
{
    public record OrderDto(Guid Id, string Sku, int Quantity, string CustomerEmail, DateTime CreatedOnUtc)
    {
        public static OrderDto From(Order order) =>
            new(order.Id, order.Sku, order.Quantity, order.CustomerEmail, order.CreatedOnUtc);
    }
}
