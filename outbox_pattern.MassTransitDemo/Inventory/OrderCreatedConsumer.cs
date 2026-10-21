using MassTransit;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Contracts;
using outbox_pattern.MassTransitDemo.Persistence;

namespace outbox_pattern.MassTransitDemo.Inventory
{
    /// <summary>
    /// Consumer của "Inventory service": nhận OrderCreated và trừ tồn kho.
    ///
    /// So với InventoryConsumer của project outbox_pattern, class này KHÔNG có code nào về inbox/transaction/khử trùng lặp.
    /// Việc đó do middleware UseEntityFrameworkOutbox (cấu hình ở Program.cs) làm xung quanh Consume():
    ///   mở transaction → ghi InboxState cho (MessageId, consumer) → nếu đã có và đã Delivered thì BỎ QUA bản trùng →
    ///   chạy Consume() → SaveChanges → commit. Consume() ném lỗi thì rollback tất cả, kể cả InboxState.
    /// </summary>
    public class OrderCreatedConsumer(AppDbContext db, ConsumerFaults faults, ILogger<OrderCreatedConsumer> logger)
        : IConsumer<OrderCreated>
    {
        public async Task Consume(ConsumeContext<OrderCreated> context)
        {
            faults.ThrowIfArmed(context.MessageId);

            var message = context.Message;

            // Cố ý dùng phép trừ không idempotent để thấy rõ: nếu thiếu inbox thì bản trùng sẽ trừ kho hai lần.
            // ExecuteUpdate chạy ngay trên transaction hiện tại của DbContext (do middleware outbox mở).
            var updated = await db.StockItems
                .Where(s => s.Sku == message.Sku && s.Available >= message.Quantity)
                .ExecuteUpdateAsync(
                    s => s.SetProperty(x => x.Available, x => x.Available - message.Quantity),
                    context.CancellationToken);

            if (updated == 0)
            {
                // Hết hàng / SKU lạ là kết quả nghiệp vụ, KHÔNG phải lỗi kỹ thuật: không ném exception (ném sẽ bị retry vô ích).
                // Hệ thống thật sẽ phát event bù trừ (vd. OrderRejected) để Orders huỷ đơn.
                logger.LogWarning(
                    "Order {OrderId}: cannot reserve {Quantity} x {Sku} (unknown SKU or insufficient stock)",
                    message.OrderId, message.Quantity, message.Sku);
            }
            else
            {
                logger.LogInformation(
                    "Order {OrderId}: reserved {Quantity} x {Sku} (message {MessageId})",
                    message.OrderId, message.Quantity, message.Sku, context.MessageId);
            }
        }
    }
}
