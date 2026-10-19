using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using outbox_pattern.Persistence;

namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Dọn các dòng outbox đã publish và các dòng inbox đã quá hạn. Nếu không dọn, hai bảng này tăng mãi.
    /// Nhiều instance cùng chạy job này vẫn an toàn: xoá là thao tác idempotent, và xoá theo lô nhỏ nên lock ngắn.
    /// </summary>
    public class MessageCleanupService(
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxOptions> options,
        ILogger<MessageCleanupService> logger) : BackgroundService
    {
        private const int DeleteBatchSize = 1000;

        private readonly OutboxOptions _options = options.Value;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var timer = new PeriodicTimer(_options.CleanupInterval);

            try
            {
                do
                {
                    try
                    {
                        await CleanupAsync(stoppingToken);
                    }
                    catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                    {
                        logger.LogError(ex, "Message cleanup failed; retrying on next tick");
                    }
                }
                while (await timer.WaitForNextTickAsync(stoppingToken));
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                // Ứng dụng đang tắt.
            }
        }

        private async Task CleanupAsync(CancellationToken cancellationToken)
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var now = DateTime.UtcNow;
            var outboxCutoff = now - _options.OutboxRetention;
            var inboxCutoff = now - _options.InboxRetention;

            int deleted;
            var outboxDeleted = 0;
            var inboxDeleted = 0;

            do
            {
                deleted = await db.OutboxMessages
                    .Where(m => m.ProcessedOnUtc < outboxCutoff)
                    .OrderBy(m => m.ProcessedOnUtc)
                    .Take(DeleteBatchSize)
                    .ExecuteDeleteAsync(cancellationToken);
                outboxDeleted += deleted;
            }
            while (deleted == DeleteBatchSize);

            do
            {
                deleted = await db.InboxMessages
                    .Where(m => m.ProcessedOnUtc < inboxCutoff)
                    .OrderBy(m => m.ProcessedOnUtc)
                    .Take(DeleteBatchSize)
                    .ExecuteDeleteAsync(cancellationToken);
                inboxDeleted += deleted;
            }
            while (deleted == DeleteBatchSize);

            if (outboxDeleted + inboxDeleted > 0)
            {
                logger.LogInformation(
                    "Cleanup removed {OutboxDeleted} outbox and {InboxDeleted} inbox rows", outboxDeleted, inboxDeleted);
            }
        }
    }
}
