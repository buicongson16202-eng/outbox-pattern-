using MassTransit;

namespace outbox_pattern.MassTransitDemo.Contracts
{
    /// <summary>
    /// Message (integration event) giữa Orders (producer) và Inventory (consumer).
    /// MassTransit mặc định định danh message bằng namespace + tên class (urn:message:namespace:Tên), nên đổi tên/namespace
    /// class sẽ làm vỡ các message đang nằm trong outbox hay trên broker. [MessageUrn] cố định tên logic, tách nó khỏi class CLR
    /// (giống hằng OrderCreatedEvent.TypeName của project outbox_pattern).
    /// </summary>
    [MessageUrn("orders.order-created.v1")]
    public record OrderCreated(Guid OrderId, string Sku, int Quantity, string CustomerEmail, DateTime OccurredOnUtc);
}
