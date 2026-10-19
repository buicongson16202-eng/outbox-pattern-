using outbox_pattern.Messaging;
using outbox_pattern.Tests.Infrastructure;

namespace outbox_pattern.Tests;

/// <summary>
/// Kịch bản 2–4: luồng POST /orders → outbox → relay → broker → consumer + inbox, gồm sự cố broker sập và mất ack.
/// Relay và consumer là OutboxProcessor / InventoryConsumer thật; broker là InMemoryBroker thật.
/// </summary>
[Collection(LocalDbCollection.Name)]
public class OutboxDeliveryTests(LocalDbFixture db) : DbTestBase(db)
{
    [Fact]
    public async Task Relay_PublishesPendingMessages_MarksThemProcessed_AndTheConsumerAppliesEachExactlyOnce()
    {
        var orders = new[]
        {
            await PostOrderAsync("SKU-001", 2),
            await PostOrderAsync("SKU-001", 3),
            await PostOrderAsync("SKU-002", 5),
        };

        // Trước khi relay chạy: cả ba đều đang "pending" và chưa có tác dụng nào lên tồn kho.
        foreach (var (_, messageId) in orders)
        {
            Assert.Null((await GetOutboxAsync(messageId)).ProcessedOnUtc);
        }

        Assert.Equal(1000, await GetStockAsync("SKU-001"));

        await using var harness = new OutboxHarness(Db);
        await harness.StartAsync();

        await Wait.UntilAsync(
            async () =>
                await GetStockAsync("SKU-001") == 995 &&
                await GetStockAsync("SKU-002") == 995 &&
                await CountInboxAsync() == 3,
            "the consumer has applied all three orders",
            diagnostics: harness.Diagnostics);

        // Cho relay thêm vài nhịp quét để chắc chắn không có lần publish thừa nào xuất hiện sau đó.
        await Task.Delay(400);

        foreach (var (_, messageId) in orders)
        {
            var row = await GetOutboxAsync(messageId);

            Assert.NotNull(row.ProcessedOnUtc);
            Assert.Equal(0, row.Attempts);
            Assert.Null(row.LastError);
            Assert.Equal(1, harness.Publisher.CallsFor(messageId));
            Assert.Equal(1, await CountInboxAsync(messageId));
        }

        Assert.Equal(995, await GetStockAsync("SKU-001"));
        Assert.Equal(995, await GetStockAsync("SKU-002"));
        Assert.Equal(1000, await GetStockAsync("SKU-003"));
        Assert.Equal(3, harness.MessagesApplied);
        Assert.Equal(0, harness.DuplicatesSkipped);
        Assert.Equal(0, harness.RelayLog.ErrorCount);
        Assert.Equal(0, harness.ConsumerLog.ErrorCount);
    }

    [Fact]
    public async Task BrokerDown_KeepsTheMessagePendingWithBackoff_AndDeliversItOnceTheBrokerIsBack()
    {
        await using var harness = new OutboxHarness(Db);
        harness.Broker.Mode = BrokerMode.Down;

        // Có outbox thì broker sập cũng không làm request hỏng (đối chứng với dual-write ngây thơ, docs mục 1.2).
        var (_, messageId) = await PostOrderAsync("SKU-001", 4);

        await harness.StartAsync();

        await Wait.UntilAsync(
            async () => (await GetOutboxAsync(messageId)).Attempts > 0,
            "the relay has tried (and failed) to publish the message",
            diagnostics: harness.Diagnostics);

        var pending = await GetOutboxAsync(messageId);
        Assert.Null(pending.ProcessedOnUtc);
        Assert.True(pending.Attempts > 0);
        Assert.True(
            pending.NextAttemptOnUtc > DateTime.UtcNow,
            $"NextAttemptOnUtc ({pending.NextAttemptOnUtc:O}) should be in the future (backoff), now is {DateTime.UtcNow:O}");
        Assert.Contains("Broker is down", pending.LastError);
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));
        Assert.Equal(0, await CountInboxAsync());

        harness.Broker.Mode = BrokerMode.Up;

        // Relay thử lại khi backoff (2 s, 4 s…) hết hạn → giao bù.
        await Wait.UntilAsync(
            async () => (await GetOutboxAsync(messageId)).ProcessedOnUtc is not null && await CountInboxAsync(messageId) == 1,
            "the relay has redelivered the message after the broker recovered",
            timeout: TimeSpan.FromSeconds(30),
            diagnostics: harness.Diagnostics);

        var delivered = await GetOutboxAsync(messageId);
        Assert.Null(delivered.LastError);
        Assert.True(delivered.Attempts > 0, "the failed attempts remain recorded for operations/alerting");
        Assert.Equal(996, await GetStockAsync("SKU-001"));
        Assert.Equal(1, harness.MessagesApplied);
        Assert.Equal(0, harness.DuplicatesSkipped);
    }

    [Fact]
    public async Task LostAck_DeliversTheSameMessageSeveralTimes_ButTheInboxDeductsStockOnlyOnce()
    {
        // Backoff ngắn để các lần gửi lại đến dồn dập trong vài trăm ms thay vì 2 s, 4 s…
        await using var harness = new OutboxHarness(Db, maxBackoff: TimeSpan.FromMilliseconds(200));
        harness.Broker.Mode = BrokerMode.LoseAck;

        var (_, messageId) = await PostOrderAsync("SKU-001", 7);

        await harness.StartAsync();

        // Broker nhận message nhưng relay luôn thấy lỗi → relay gửi lại cùng một MessageId.
        await Wait.UntilAsync(
            () => Task.FromResult(harness.Publisher.CallsFor(messageId) >= 3),
            "the relay has published the same message at least three times",
            diagnostics: harness.Diagnostics);

        harness.Broker.Mode = BrokerMode.Up;

        await Wait.UntilAsync(
            async () => (await GetOutboxAsync(messageId)).ProcessedOnUtc is not null,
            "the relay finally got an ack",
            diagnostics: harness.Diagnostics);

        // Mỗi lần publish (kể cả lần mất ack) đều đặt một bản vào hàng đợi của broker → consumer phải nhận từng bản.
        var deliveries = harness.Publisher.CallsFor(messageId);
        Assert.True(deliveries >= 4, $"expected at least 3 lost acks + 1 successful publish, got {deliveries}");

        await Wait.UntilAsync(
            () => Task.FromResult(harness.MessagesApplied + harness.DuplicatesSkipped == deliveries),
            $"the consumer has handled all {deliveries} deliveries",
            diagnostics: harness.Diagnostics);

        Assert.Equal(1, harness.MessagesApplied);
        Assert.Equal(deliveries - 1, harness.DuplicatesSkipped);
        Assert.Equal(1, await CountInboxAsync(messageId));
        Assert.Equal(993, await GetStockAsync("SKU-001")); // trừ đúng 7, không phải 7 × deliveries
        Assert.Equal(0, harness.ConsumerLog.ErrorCount);

        var row = await GetOutboxAsync(messageId);
        Assert.True(row.Attempts >= 3);
        Assert.Null(row.LastError);
    }
}
