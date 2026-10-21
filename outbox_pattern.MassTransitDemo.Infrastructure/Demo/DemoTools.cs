using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Domain.Orders;
using outbox_pattern.MassTransitDemo.Infrastructure.Messaging;
using outbox_pattern.MassTransitDemo.Infrastructure.Persistence;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Demo
{
    /// <summary>
    /// Công cụ chỉ dùng cho các endpoint /demo (học/quan sát). Nằm ở Infrastructure vì nó đọc thẳng ba bảng của MassTransit
    /// và cố ý đi vòng qua use case để tạo bản trùng — những việc mà code nghiệp vụ không bao giờ được làm.
    /// </summary>
    public class DemoTools(AppDbContext db, IPublishEndpoint publishEndpoint, ConsumerFaults faults, TransportSwitch transport)
    {
        /// <summary>
        /// Ảnh chụp trạng thái: order, ba bảng của MassTransit (OutboxMessage, OutboxState, InboxState) và tồn kho.
        /// outbox.waitingMessages = số dòng đang chờ giao (dòng đã giao được dịch vụ giao outbox xoá đi).
        /// </summary>
        public async Task<object> GetStateAsync(CancellationToken cancellationToken)
        {
            var outboxMessages = db.Set<OutboxMessage>().AsNoTracking();
            var outboxStates = db.Set<OutboxState>().AsNoTracking();
            var inbox = db.Set<InboxState>().AsNoTracking();

            return new
            {
                transport = transport.Mode,
                orders = await db.Orders.CountAsync(cancellationToken),
                outbox = new
                {
                    waitingMessages = await outboxMessages.CountAsync(cancellationToken),
                    waiting = await outboxMessages
                        .OrderBy(m => m.SequenceNumber)
                        .Take(10)
                        .Select(m => new { m.SequenceNumber, m.MessageId, m.MessageType, m.EnqueueTime })
                        .ToListAsync(cancellationToken),
                    states = await outboxStates.CountAsync(cancellationToken),
                    undeliveredStates = await outboxStates.CountAsync(s => s.Delivered == null, cancellationToken),
                },
                inbox = new
                {
                    total = await inbox.CountAsync(cancellationToken),
                    consumed = await inbox.CountAsync(i => i.Consumed != null, cancellationToken),
                    receiveCount = await inbox.SumAsync(i => i.ReceiveCount, cancellationToken),
                },
                consumerFailuresArmed = faults.Remaining,
                stock = await db.StockItems
                    .AsNoTracking()
                    .OrderBy(s => s.Sku)
                    .Select(s => new { s.Sku, s.Available })
                    .ToListAsync(cancellationToken),
            };
        }

        /// <summary>
        /// Tái hiện bản trùng: tạo một order nhưng đưa CÙNG MỘT message (cùng MessageId) vào outbox nhiều lần,
        /// như khi broker giao lại. Consumer vẫn chỉ trừ kho một lần nhờ inbox.
        /// </summary>
        public async Task<(Guid OrderId, Guid MessageId)> CreateOrderWithDuplicateMessagesAsync(
            string sku, int quantity, string customerEmail, int copies, CancellationToken cancellationToken)
        {
            var order = Order.Create(sku, quantity, customerEmail, DateTime.UtcNow);
            var messageId = Guid.NewGuid();

            db.Orders.Add(order);

            for (var i = 0; i < copies; i++)
            {
                await publishEndpoint.Publish(
                    new OrderCreated(order.Id, order.Sku, order.Quantity, order.CustomerEmail, order.CreatedOnUtc),
                    context => context.MessageId = messageId,
                    cancellationToken);
            }

            await db.SaveChangesAsync(cancellationToken);

            return (order.Id, messageId);
        }
    }
}
