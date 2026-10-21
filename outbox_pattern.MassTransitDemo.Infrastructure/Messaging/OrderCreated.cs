using MassTransit;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Messaging
{
    /// <summary>
    /// Hợp đồng message trên "dây" giữa Orders (producer) và Inventory (consumer). Nằm ở Infrastructure vì đây là định dạng
    /// truyền tải — giống schema của bảng — chứ không phải nghiệp vụ; Application chỉ biết cổng IOrderEventPublisher.
    /// (Khi tách thành nhiều service thật, record này thường được đưa vào một package "Contracts" dùng chung.)
    ///
    /// MassTransit mặc định định danh message bằng namespace + tên class (urn:message:namespace:Tên), nên đổi tên/namespace
    /// class sẽ làm vỡ các message đang nằm trong outbox hay trên broker. [MessageUrn] cố định tên logic, tách nó khỏi class CLR
    /// (nhờ vậy việc chuyển class này sang project khác khi tách Clean Architecture không đổi định danh của message).
    /// </summary>
    [MessageUrn("orders.order-created.v1")]
    public record OrderCreated(Guid OrderId, string Sku, int Quantity, string CustomerEmail, DateTime OccurredOnUtc);
}
