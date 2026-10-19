using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.Contracts;
using outbox_pattern.Domain;
using outbox_pattern.Messaging;
using outbox_pattern.Persistence;

namespace outbox_pattern.Inventory
{
    /// <summary>
    /// Consumer của "Inventory service": nhận OrderCreated và trừ tồn kho.
    /// Outbox + broker chỉ bảo đảm at-least-once nên cùng một message có thể đến NHIỀU lần;
    /// bảng inbox biến "giao ít nhất một lần" thành "có tác dụng đúng một lần".
    /// </summary>
    public class InventoryConsumer(
        InMemoryBroker broker,
        IServiceScopeFactory scopeFactory,
        ILogger<InventoryConsumer> logger) : BackgroundService
    {
        private const string ConsumerName = "inventory";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await foreach (var message in broker.ReadAllAsync(stoppingToken))
                {
                    try
                    {
                        await HandleAsync(message, stoppingToken);
                    }
                    catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Broker thật sẽ tự giao lại message chưa được ack (và đưa vào DLQ sau N lần).
                        // Broker giả lập thì mô phỏng bằng cách đưa message trở lại hàng đợi sau một nhịp.
                        logger.LogError(ex, "Handling message {MessageId} failed; requeueing", message.Id);
                        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                        await broker.RequeueAsync(message, stoppingToken);
                    }
                }
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                // Ứng dụng đang tắt.
            }
        }

        private async Task HandleAsync(BrokerMessage message, CancellationToken cancellationToken)
        {
            if (message.Type != OrderCreatedEvent.TypeName)
            {
                logger.LogWarning("Ignoring message {MessageId} of unknown type {Type}", message.Id, message.Type);
                return;
            }

            var orderCreated = JsonSerializer.Deserialize<OrderCreatedEvent>(message.Payload)
                ?? throw new InvalidOperationException($"Message {message.Id} has an empty payload");

            await using var scope = scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

            // (1) Ghi dấu "đã xử lý" TRƯỚC. Khoá chính (MessageId, Consumer) là chốt chặn cuối cùng: bản trùng sẽ vi phạm
            //     khoá — kể cả khi hai instance xử lý cùng một message đồng thời, database chỉ cho đúng một bên thắng.
            db.InboxMessages.Add(new InboxMessage
            {
                MessageId = message.Id,
                Consumer = ConsumerName,
                ProcessedOnUtc = DateTime.UtcNow,
            });

            try
            {
                await db.SaveChangesAsync(cancellationToken);
            }
            catch (DbUpdateException ex) when (IsUniqueViolation(ex))
            {
                logger.LogInformation("Duplicate message {MessageId} skipped (already processed)", message.Id);
                return;
            }

            // (2) Tác dụng nghiệp vụ, nằm CÙNG transaction với bước (1): hoặc cả hai cùng có hiệu lực, hoặc không cái nào.
            //     Cố ý dùng phép trừ không idempotent để thấy rõ: nếu thiếu inbox thì bản trùng sẽ trừ kho hai lần.
            var updated = await db.StockItems
                .Where(s => s.Sku == orderCreated.Sku && s.Available >= orderCreated.Quantity)
                .ExecuteUpdateAsync(
                    s => s.SetProperty(x => x.Available, x => x.Available - orderCreated.Quantity),
                    cancellationToken);

            if (updated == 0)
            {
                // Hết hàng / SKU lạ là kết quả nghiệp vụ, KHÔNG phải lỗi kỹ thuật: vẫn ghi inbox để khỏi thử lại vô ích.
                // Hệ thống thật sẽ phát event bù trừ (vd. OrderRejected) để Orders huỷ đơn.
                logger.LogWarning(
                    "Order {OrderId}: cannot reserve {Quantity} x {Sku} (unknown SKU or insufficient stock)",
                    orderCreated.OrderId, orderCreated.Quantity, orderCreated.Sku);
            }
            else
            {
                logger.LogInformation(
                    "Order {OrderId}: reserved {Quantity} x {Sku} (message {MessageId})",
                    orderCreated.OrderId, orderCreated.Quantity, orderCreated.Sku, message.Id);
            }

            await transaction.CommitAsync(cancellationToken);
        }

        // 2601 = duplicate key trên unique index, 2627 = vi phạm PRIMARY KEY / UNIQUE constraint.
        private static bool IsUniqueViolation(DbUpdateException exception) =>
            exception.InnerException is SqlException { Number: 2601 or 2627 };
    }
}
