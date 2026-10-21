using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Application.Abstractions
{
    // Các "cổng" (port) của tầng Application. Infrastructure cài đặt chúng bằng EF Core và MassTransit;
    // use case chỉ nói chuyện với các interface này nên chạy được với bản giả trong unit test.

    public interface IOrderRepository
    {
        void Add(Order order);

        Task<Order?> GetByIdAsync(Guid id, CancellationToken cancellationToken);
    }

    public interface IStockRepository
    {
        /// <summary>
        /// Giữ chỗ <paramref name="quantity"/> sản phẩm nếu còn đủ, NGUYÊN TỬ (an toàn khi nhiều consumer chạy đồng thời).
        /// Trả về false nếu SKU không tồn tại hoặc không đủ hàng.
        /// </summary>
        Task<bool> TryReserveAsync(string sku, int quantity, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Phát event về order cho các service khác. Đây là cổng của OUTBOX: cài đặt chỉ GHI NHẬN message vào cùng unit of work,
    /// message chỉ thực sự được lưu (và sau đó mới được gửi đi) khi <see cref="IUnitOfWork.SaveChangesAsync"/> commit.
    /// </summary>
    public interface IOrderEventPublisher
    {
        Task PublishOrderCreatedAsync(Order order, CancellationToken cancellationToken);
    }

    /// <summary>Commit MỘT transaction gồm mọi thay đổi đang chờ: entity nghiệp vụ và các message đã "publish" qua outbox.</summary>
    public interface IUnitOfWork
    {
        Task<int> SaveChangesAsync(CancellationToken cancellationToken);
    }
}
