using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using outbox_pattern.Contracts;
using outbox_pattern.Inventory;
using outbox_pattern.Messaging;
using outbox_pattern.Tests.Infrastructure;

namespace outbox_pattern.Tests;

/// <summary>
/// Kịch bản 5–6: nhiều relay / nhiều consumer tranh nhau trên cùng một database (docs mục 9, case P1 và C2).
/// Mỗi relay là một OutboxProcessor riêng; mỗi lô xử lý tạo scope + DbContext + transaction riêng (xem ProcessBatchAsync).
/// </summary>
[Collection(LocalDbCollection.Name)]
public class ConcurrencyTests(LocalDbFixture db) : DbTestBase(db)
{
    private const int RelayCount = 4;
    private const int PendingRows = 300;

    [Fact]
    public async Task ManyRelaysClaimingTheSamePendingRows_PublishEveryMessageExactlyOnce()
    {
        var messageIds = await SeedPendingOutboxRowsAsync(PendingRows);

        var publishes = new ConcurrentDictionary<Guid, ConcurrentBag<int>>(); // MessageId -> các relay đã publish nó
        var relayLog = new ListLogger<OutboxProcessor>();

        await using var services = Db.CreateServices();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();

        var relays = Enumerable.Range(0, RelayCount)
            .Select(index => new OutboxProcessor(scopes, new ContendedPublisher(index, publishes), TestOptions.Outbox(), relayLog))
            .ToList();

        try
        {
            await Task.WhenAll(relays.Select(relay => relay.StartAsync(CancellationToken.None)));

            // READ UNCOMMITTED: chỉ để thăm dò tiến độ mà không chen vào khoá của các relay.
            await Wait.UntilAsync(
                async () => await Db.ScalarAsync<int>(
                    "SELECT COUNT(*) FROM OutboxMessages WITH (NOLOCK) WHERE ProcessedOnUtc IS NULL") == 0,
                $"all {PendingRows} pending rows have been marked processed",
                timeout: TimeSpan.FromSeconds(60),
                diagnostics: relayLog.Dump);
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await Task.WhenAll(relays.Select(relay => relay.StopAsync(timeout.Token)));
            relays.ForEach(relay => relay.Dispose());
        }

        // Mỗi message được publish đúng một lần (bởi đúng một relay) — không mất, không trùng.
        var totalPublishes = publishes.Values.Sum(relaysThatPublished => relaysThatPublished.Count);
        if (totalPublishes != PendingRows || publishes.Count != PendingRows)
        {
            Assert.Fail(
                $"Expected {PendingRows} publishes of {PendingRows} distinct messages, got {totalPublishes} publishes of {publishes.Count} " +
                $"({publishes.Count(p => p.Value.Count > 1)} messages published more than once).{Environment.NewLine}{relayLog.Dump()}");
        }

        Assert.All(messageIds, id => Assert.Single(publishes[id]));
        Assert.Equal(0, relayLog.ErrorCount);

        await using var context = Db.CreateDbContext();
        var rows = await context.OutboxMessages.AsNoTracking().ToListAsync();
        Assert.Equal(PendingRows, rows.Count);
        Assert.All(rows, row =>
        {
            Assert.NotNull(row.ProcessedOnUtc);
            Assert.Equal(0, row.Attempts);
            Assert.Null(row.LastError);
        });
    }

    [Fact]
    public async Task TwoConsumersHandlingTheSameMessageAtTheSameTime_OnlyOneAppliesIt()
    {
        var logger = new ListLogger<InventoryConsumer>();
        var message = NewOrderMessage(quantity: 5);

        await using var services = Db.CreateServices();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();

        // Mỗi consumer có broker riêng và cùng nhận BẢN SAO của một message (cùng MessageId), như khi relay publish trùng.
        var brokers = new[] { NewBroker(), NewBroker() };
        foreach (var broker in brokers)
        {
            await broker.PublishAsync(message, CancellationToken.None);
        }

        var consumers = brokers.Select(broker => new InventoryConsumer(broker, scopes, logger)).ToList();

        // Cách làm cho hai consumer CHẮC CHẮN chồng lên nhau: giữ khoá dòng tồn kho trong một transaction của test.
        //   consumer A: INSERT inbox (đã giữ khoá khoá chính) → UPDATE stock → bị chặn bởi test
        //   consumer B: INSERT inbox cùng khoá chính          → bị chặn bởi A
        await using var blocker = new SqlConnection(Db.ConnectionString);
        await blocker.OpenAsync();
        await using var blockerTransaction = (SqlTransaction)await blocker.BeginTransactionAsync();
        await using (var hold = new SqlCommand(
            "UPDATE StockItems SET Available = Available WHERE Sku = N'SKU-001'", blocker, blockerTransaction))
        {
            await hold.ExecuteNonQueryAsync();
        }

        try
        {
            await Task.WhenAll(consumers.Select(consumer => consumer.StartAsync(CancellationToken.None)));

            await Wait.UntilAsync(
                async () => await Db.ScalarAsync<int>(
                    "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE database_id = DB_ID() AND blocking_session_id > 0") >= 2,
                "both consumers are mid-transaction and blocked (one on the stock row, one on the inbox key)",
                timeout: TimeSpan.FromSeconds(20),
                diagnostics: logger.Dump);

            // Hai bên đang cùng xử lý một MessageId và chưa bên nào commit → chưa có gì được áp dụng.
            Assert.Equal(0, logger.Count(": reserved "));

            await blockerTransaction.CommitAsync();

            await Wait.UntilAsync(
                () => Task.FromResult(logger.Count(": reserved ") + logger.Count("Duplicate message") >= 2),
                "both consumers have finished handling the message",
                diagnostics: logger.Dump);
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await Task.WhenAll(consumers.Select(consumer => consumer.StopAsync(timeout.Token)));
            consumers.ForEach(consumer => consumer.Dispose());
        }

        Assert.Equal(1, logger.Count(": reserved "));
        Assert.Equal(1, logger.Count("Duplicate message"));
        Assert.Equal(0, logger.ErrorCount);
        Assert.Equal(1, await CountInboxAsync(message.Id));
        Assert.Equal(995, await GetStockAsync("SKU-001")); // trừ 5 một lần, không phải 10
    }

    [Fact]
    public async Task ManyMessagesDeliveredToTwoConsumersAtOnce_EachMessageIsAppliedExactlyOnce()
    {
        const int messageCount = 50;

        var logger = new ListLogger<InventoryConsumer>();
        var messages = Enumerable.Range(0, messageCount).Select(_ => NewOrderMessage(quantity: 1)).ToList();

        await using var services = Db.CreateServices();
        var scopes = services.GetRequiredService<IServiceScopeFactory>();

        var brokers = new[] { NewBroker(), NewBroker() };
        foreach (var broker in brokers)
        {
            foreach (var message in messages)
            {
                await broker.PublishAsync(message, CancellationToken.None);
            }
        }

        var consumers = brokers.Select(broker => new InventoryConsumer(broker, scopes, logger)).ToList();

        try
        {
            await Task.WhenAll(consumers.Select(consumer => consumer.StartAsync(CancellationToken.None)));

            await Wait.UntilAsync(
                () => Task.FromResult(logger.Count(": reserved ") + logger.Count("Duplicate message") >= 2 * messageCount),
                $"both consumers have handled all {messageCount} messages",
                timeout: TimeSpan.FromSeconds(60),
                diagnostics: logger.Dump);
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await Task.WhenAll(consumers.Select(consumer => consumer.StopAsync(timeout.Token)));
            consumers.ForEach(consumer => consumer.Dispose());
        }

        Assert.Equal(messageCount, logger.Count(": reserved "));
        Assert.Equal(messageCount, logger.Count("Duplicate message"));
        Assert.Equal(0, logger.ErrorCount);
        Assert.Equal(messageCount, await CountInboxAsync());
        Assert.Equal(LocalDbFixture.InitialStock - messageCount, await GetStockAsync("SKU-001"));
    }

    private static InMemoryBroker NewBroker() => new(NullLogger<InMemoryBroker>.Instance);

    private static BrokerMessage NewOrderMessage(int quantity)
    {
        var now = DateTime.UtcNow;
        var orderId = Guid.NewGuid();
        var payload = JsonSerializer.Serialize(new OrderCreatedEvent(orderId, "SKU-001", quantity, "buyer@example.com", now));

        return new BrokerMessage(Guid.NewGuid(), OrderCreatedEvent.TypeName, orderId.ToString(), payload, now);
    }

    private async Task<List<Guid>> SeedPendingOutboxRowsAsync(int count)
    {
        // Lùi mốc thời gian để mọi dòng đều đã "đến hạn" (NextAttemptOnUtc <= now) khi relay claim.
        var start = DateTime.UtcNow.AddMinutes(-1);

        var rows = Enumerable.Range(0, count)
            .Select(i =>
            {
                var at = start.AddMilliseconds(i);
                var orderId = Guid.NewGuid();
                return OutboxMessage.Create(
                    OrderCreatedEvent.TypeName,
                    orderId.ToString(),
                    new OrderCreatedEvent(orderId, "SKU-001", 1, "buyer@example.com", at),
                    at);
            })
            .ToList();

        await using var context = Db.CreateDbContext();
        context.OutboxMessages.AddRange(rows);
        await context.SaveChangesAsync();

        return rows.Select(row => row.Id).ToList();
    }

    /// <summary>
    /// Publisher của một relay: ghi lại relay nào đã publish message nào. Lần publish ĐẦU TIÊN của mỗi relay bị giữ lại một lúc
    /// (relay vẫn đang mở transaction và giữ khoá các dòng nó đã claim) để các relay còn lại — quét mỗi 50 ms — chắc chắn chạy
    /// truy vấn claim của chúng ĐÚNG LÚC dòng đó đang bị khoá; đó là khoảnh khắc sinh ra bản trùng nếu thiếu UPDLOCK/READPAST.
    /// Các lần publish sau nghỉ một chút để các lô trải dài hơn.
    /// </summary>
    private sealed class ContendedPublisher(int relayIndex, ConcurrentDictionary<Guid, ConcurrentBag<int>> publishes)
        : IMessagePublisher
    {
        private int _published;

        public async ValueTask PublishAsync(BrokerMessage message, CancellationToken cancellationToken)
        {
            var first = Interlocked.Increment(ref _published) == 1;
            await Task.Delay(first ? TimeSpan.FromMilliseconds(500) : TimeSpan.FromMilliseconds(1), cancellationToken);

            publishes.GetOrAdd(message.Id, _ => new ConcurrentBag<int>()).Add(relayIndex);
        }
    }
}
