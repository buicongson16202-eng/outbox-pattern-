using MassTransit;
using Microsoft.Extensions.Logging;
using outbox_pattern.MassTransitDemo.Application.Inventory;
using outbox_pattern.MassTransitDemo.Infrastructure.Demo;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Messaging
{
    /// <summary>
    /// Adapter phía Inventory: nhận message OrderCreated từ MassTransit rồi gọi use case ReserveStockHandler của Application.
    ///
    /// Không có code nào về inbox/transaction/khử trùng lặp. Việc đó do middleware UseEntityFrameworkOutbox (DependencyInjection.cs)
    /// làm xung quanh Consume(): mở transaction → ghi InboxState cho (MessageId, consumer) → nếu đã có và đã Delivered thì
    /// BỎ QUA bản trùng → chạy Consume() → SaveChanges → commit. Consume() ném lỗi thì rollback tất cả, kể cả InboxState.
    /// </summary>
    public class OrderCreatedConsumer(ReserveStockHandler reserveStock, ConsumerFaults faults, ILogger<OrderCreatedConsumer> logger)
        : IConsumer<OrderCreated>
    {
        public async Task Consume(ConsumeContext<OrderCreated> context)
        {
            faults.ThrowIfArmed(context.MessageId); // công tắc demo, mặc định tắt

            var message = context.Message;

            var result = await reserveStock.HandleAsync(
                new ReserveStockCommand(message.OrderId, message.Sku, message.Quantity),
                context.CancellationToken);

            if (result == ReserveStockResult.Rejected)
            {
                // Hết hàng / SKU lạ là kết quả nghiệp vụ, KHÔNG ném exception (ném sẽ bị retry vô ích).
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
