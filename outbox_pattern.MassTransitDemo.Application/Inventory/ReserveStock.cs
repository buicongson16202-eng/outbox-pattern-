using outbox_pattern.MassTransitDemo.Application.Abstractions;

namespace outbox_pattern.MassTransitDemo.Application.Inventory
{
    public record ReserveStockCommand(Guid OrderId, string Sku, int Quantity);

    public enum ReserveStockResult
    {
        Reserved,

        /// <summary>Hết hàng hoặc SKU lạ: kết quả nghiệp vụ, KHÔNG phải lỗi kỹ thuật nên không được retry.</summary>
        Rejected,
    }

    /// <summary>
    /// Use case "giữ hàng cho order", chạy khi Inventory nhận OrderCreated. Không có code inbox/transaction ở đây:
    /// việc khử trùng lặp và bọc transaction do adapter consumer ở Infrastructure (middleware outbox của MassTransit) lo.
    /// </summary>
    public class ReserveStockHandler(IStockRepository stock)
    {
        public async Task<ReserveStockResult> HandleAsync(ReserveStockCommand command, CancellationToken cancellationToken) =>
            await stock.TryReserveAsync(command.Sku, command.Quantity, cancellationToken)
                ? ReserveStockResult.Reserved
                : ReserveStockResult.Rejected;
    }
}
