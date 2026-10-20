using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using outbox_pattern.Persistence;

namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Relay (polling publisher): đọc các dòng outbox chưa gửi và đẩy sang broker.
    /// Mọi instance của service đều chạy một relay; chúng phối hợp với nhau bằng "lease" (UPDATE nguyên tử ngắn),
    /// không giữ lock/transaction trong lúc gọi broker, không cần leader election.
    ///
    /// Bảo đảm: at-least-once. Nếu relay chết SAU khi broker đã nhận nhưng TRƯỚC khi đánh dấu đã xử lý, hoặc lease hết hạn
    /// khi relay quá chậm, message sẽ được gửi lại → consumer bắt buộc phải idempotent (xem InventoryConsumer + bảng inbox).
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

            var claimed = await ClaimBatchAsync(db, stoppingToken);

            if (claimed.Count == 0)
            {
                return 0;
            }

            var published = 0;
            var handled = 0; // claimed[handled..] là phần chưa xử lý, sẽ được trả lại ở cuối hàm

            try
            {
                foreach (var message in claimed)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    // Publish nằm NGOÀI mọi transaction: không có lock nào bị giữ trong lúc chờ broker.
                    using var publishTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    publishTimeout.CancelAfter(_options.PublishTimeout);

                    try
                    {
                        await publisher.PublishAsync(
                            new BrokerMessage(message.Id, message.Type, message.AggregateId, message.Payload, message.OccurredOnUtc),
                            publishTimeout.Token);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        // Đang tắt ứng dụng: chưa biết message này đã tới broker hay chưa nên coi như chưa xử lý.
                        // Tệ nhất là nó bị gửi lại sau này — chấp nhận được vì consumer idempotent.
                        break;
                    }
                    catch (Exception ex)
                    {
                        await MarkFailedAsync(db, message, ex);
                        handled++;

                        // Broker nhiều khả năng đang gặp sự cố → dừng lô tại đây để không "đốt" lượt thử của những message
                        // phía sau. Message lỗi đã bị đẩy lùi bởi backoff nên lần quét sau relay sẽ đi tiếp những message
                        // khác (một message "độc" không chặn cả hàng đợi).
                        break;
                    }

                    await MarkProcessedAsync(db, message);
                    handled++;
                    published++;
                }
            }
            finally
            {
                // Trả lại lease của phần chưa xử lý để instance khác nhặt ngay, khỏi phải chờ lease hết hạn.
                await ReleaseAsync(db, claimed.Skip(handled));
            }

            logger.LogInformation("Outbox relay: claimed {Claimed}, published {Published}", claimed.Count, published);

            return published;
        }

        /// <summary>
        /// "Claim" bằng lease thay vì giữ lock: đọc ứng viên (không khoá), rồi với từng dòng chạy MỘT câu UPDATE nguyên tử
        /// đẩy NextAttemptOnUtc ra xa (compare-and-swap trên chính giá trị đã đọc). Câu UPDATE chỉ giữ khoá dòng trong vài ms;
        /// ai cập nhật được dòng (1 row affected) thì sở hữu lease, relay khác thấy 0 row và bỏ qua dòng đó.
        /// Relay chết giữa chừng thì lease tự hết hạn và message được thử lại — không cần transaction hay lock kéo dài.
        /// </summary>
        private async Task<List<OutboxMessage>> ClaimBatchAsync(AppDbContext db, CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            var candidates = await db.OutboxMessages
                .AsNoTracking()
                .Where(m => m.ProcessedOnUtc == null && m.NextAttemptOnUtc <= now)
                .OrderBy(m => m.NextAttemptOnUtc).ThenBy(m => m.OccurredOnUtc)
                .Take(_options.BatchSize)
                .ToListAsync(cancellationToken);

            var claimed = new List<OutboxMessage>(candidates.Count);

            foreach (var candidate in candidates)
            {
                var id = candidate.Id;
                var expected = candidate.NextAttemptOnUtc;
                var leaseUntil = DateTime.UtcNow + _options.ClaimLease;

                var won = await db.OutboxMessages
                    .Where(m => m.Id == id && m.ProcessedOnUtc == null && m.NextAttemptOnUtc == expected)
                    .ExecuteUpdateAsync(s => s.SetProperty(m => m.NextAttemptOnUtc, leaseUntil), cancellationToken);

                if (won == 1)
                {
                    claimed.Add(candidate);
                }
            }

            return claimed;
        }

        // Các bước ghi kết quả dùng CancellationToken.None để dù đang tắt ứng dụng, kết quả của message ĐÃ publish vẫn được lưu
        // (giảm số bản trùng).
        private static Task MarkProcessedAsync(AppDbContext db, OutboxMessage message)
        {
            var processedOn = DateTime.UtcNow;

            return db.OutboxMessages
                .Where(m => m.Id == message.Id)
                .ExecuteUpdateAsync(
                    s => s.SetProperty(m => m.ProcessedOnUtc, processedOn).SetProperty(m => m.LastError, (string?)null),
                    CancellationToken.None);
        }

        private async Task MarkFailedAsync(AppDbContext db, OutboxMessage message, Exception exception)
        {
            var attempts = message.Attempts + 1;
            var error = Truncate(exception.Message, 2000);
            var nextAttempt = DateTime.UtcNow + Backoff(attempts);

            await db.OutboxMessages
                .Where(m => m.Id == message.Id)
                .ExecuteUpdateAsync(
                    s => s.SetProperty(m => m.Attempts, attempts)
                        .SetProperty(m => m.LastError, error)
                        .SetProperty(m => m.NextAttemptOnUtc, nextAttempt),
                    CancellationToken.None);

            logger.LogWarning(
                "Publishing outbox message {MessageId} failed (attempt {Attempts}), next attempt at {NextAttemptOnUtc:O}: {Error}",
                message.Id, attempts, nextAttempt, exception.Message);
        }

        // Khôi phục NextAttemptOnUtc ban đầu (giữ nguyên thứ tự ưu tiên) cho những dòng đã claim nhưng chưa publish.
        private static async Task ReleaseAsync(AppDbContext db, IEnumerable<OutboxMessage> unhandled)
        {
            foreach (var message in unhandled)
            {
                var original = message.NextAttemptOnUtc;

                await db.OutboxMessages
                    .Where(m => m.Id == message.Id && m.ProcessedOnUtc == null)
                    .ExecuteUpdateAsync(s => s.SetProperty(m => m.NextAttemptOnUtc, original), CancellationToken.None);
            }
        }

        private TimeSpan Backoff(int attempts) =>
            TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempts), _options.MaxBackoff.TotalSeconds));

        private static string Truncate(string value, int maxLength) =>
            value.Length <= maxLength ? value : value[..maxLength];
    }
}
