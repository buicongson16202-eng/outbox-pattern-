using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using outbox_pattern.Persistence;

namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Relay (polling publisher): đọc các dòng outbox chưa gửi và đẩy sang broker.
    /// Mọi instance của service đều chạy một relay; chúng phối hợp với nhau CHỈ bằng row-lock của database,
    /// không cần leader election hay bất kỳ thành phần điều phối nào khác.
    ///
    /// Bảo đảm: at-least-once. Nếu relay chết SAU khi broker đã nhận nhưng TRƯỚC khi commit, message sẽ được
    /// gửi lại → consumer bắt buộc phải idempotent (xem InventoryConsumer + bảng inbox).
    /// </summary>
    public class OutboxProcessor(
        IServiceScopeFactory scopeFactory,
        IMessagePublisher publisher,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessor> logger) : BackgroundService
    {
        private readonly OutboxOptions _options = options.Value;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var timer = new PeriodicTimer(_options.PollingInterval);

            try
            {
                do
                {
                    try
                    {
                        // Xử lý trọn một lô tức là có thể còn việc → làm tiếp ngay; chỉ ngủ khi đã hết hoặc gặp lỗi.
                        while (await ProcessBatchAsync(stoppingToken) == _options.BatchSize)
                        {
                        }
                    }
                    catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                    {
                        logger.LogError(ex, "Outbox relay iteration failed; retrying on next tick");
                    }
                }
                while (await timer.WaitForNextTickAsync(stoppingToken));
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                // Ứng dụng đang tắt.
            }
        }

        /// <returns>Số message đã publish thành công trong lô.</returns>
        private async Task<int> ProcessBatchAsync(CancellationToken stoppingToken)
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            // Transaction này giữ row-lock trong suốt thời gian xử lý lô; commit/rollback sẽ nhả lock.
            // Nếu process chết giữa chừng, SQL Server tự rollback và các dòng quay lại trạng thái "chưa xử lý".
            await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);

            // "Claim" một lô message bằng row-lock:
            //   UPDLOCK  - khoá các dòng này cho riêng mình đến khi commit
            //   READPAST - dòng đang bị relay khác khoá thì BỎ QUA thay vì chờ → các instance không giẫm chân nhau
            //   ROWLOCK  - khoá mức dòng, tránh leo thang thành khoá cả bảng
            // (PostgreSQL / MySQL 8 / Oracle tương đương: SELECT ... FOR UPDATE SKIP LOCKED)
            var batch = await db.OutboxMessages
                .FromSql($"""
                    SELECT TOP ({_options.BatchSize}) *
                    FROM OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK)
                    WHERE ProcessedOnUtc IS NULL AND NextAttemptOnUtc <= {DateTime.UtcNow}
                    ORDER BY NextAttemptOnUtc, OccurredOnUtc
                    """)
                .ToListAsync(stoppingToken);

            if (batch.Count == 0)
            {
                return 0;
            }

            var published = 0;

            foreach (var message in batch)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    // Đang tắt ứng dụng: không đụng tới phần còn lại của lô, commit bên dưới sẽ nhả chúng cho instance khác.
                    break;
                }

                using var publishTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                publishTimeout.CancelAfter(_options.PublishTimeout);

                try
                {
                    await publisher.PublishAsync(
                        new BrokerMessage(message.Id, message.Type, message.AggregateId, message.Payload, message.OccurredOnUtc),
                        publishTimeout.Token);

                    message.ProcessedOnUtc = DateTime.UtcNow;
                    message.LastError = null;
                    published++;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Đang tắt ứng dụng: chưa biết message này đã tới broker hay chưa nên để nguyên "chưa xử lý".
                    // Tệ nhất là nó bị gửi lại sau này — chấp nhận được vì consumer idempotent.
                    break;
                }
                catch (Exception ex)
                {
                    message.Attempts++;
                    message.LastError = Truncate(ex.Message, 2000);
                    message.NextAttemptOnUtc = DateTime.UtcNow + Backoff(message.Attempts);

                    logger.LogWarning(
                        "Publishing outbox message {MessageId} failed (attempt {Attempts}), next attempt at {NextAttemptOnUtc:O}: {Error}",
                        message.Id, message.Attempts, message.NextAttemptOnUtc, ex.Message);

                    // Broker nhiều khả năng đang gặp sự cố → dừng lô tại đây để không giữ transaction/lock lâu
                    // và không "đốt" lượt thử của những message phía sau. Message lỗi đã bị đẩy lùi bởi backoff
                    // nên lần quét sau relay sẽ đi tiếp những message khác (một message "độc" không chặn cả hàng đợi).
                    break;
                }
            }

            // Ghi kết quả và nhả lock trong cùng transaction. Dùng CancellationToken.None để dù đang tắt ứng dụng,
            // kết quả của những message ĐÃ publish vẫn được lưu (giảm số bản trùng).
            await db.SaveChangesAsync(CancellationToken.None);
            await transaction.CommitAsync(CancellationToken.None);

            logger.LogInformation("Outbox relay: claimed {Claimed}, published {Published}", batch.Count, published);

            return published;
        }

        private TimeSpan Backoff(int attempts) =>
            TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempts), _options.MaxBackoff.TotalSeconds));

        private static string Truncate(string value, int maxLength) =>
            value.Length <= maxLength ? value : value[..maxLength];
    }
}
