using System.Net.Http.Json;
using outbox_pattern.MassTransitDemo.Tests.Infrastructure;

namespace outbox_pattern.MassTransitDemo.Tests;

/// <summary>
/// Luồng POST /orders → OutboxMessage → dịch vụ giao outbox của MassTransit → transport → consumer + InboxState,
/// gồm các sự cố: transport sập, consumer lỗi, bản trùng (docs/masstransit.md mục 4.2–4.5).
/// Mọi thứ phía MassTransit (dịch vụ giao outbox, bus, middleware inbox) là thật; chỉ transport là in-memory.
/// </summary>
[Collection(LocalDbCollection.Name)]
public class DeliveryAndInboxTests(LocalDbFixture db) : DbTestBase(db)
{
    [Fact]
    public async Task EndToEnd_DeliversTheOutboxMessage_DeletesIt_AndTheConsumerAppliesItOnce()
    {
        var orderId = await PostOrderAsync("SKU-001", 2);

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-001") == 998,
            "the consumer has deducted the stock",
            diagnostics: () => Api.Logs.Dump());

        // Dòng outbox đã giao bị dịch vụ giao outbox XOÁ (khác bảng tự viết của project outbox_pattern, nơi dòng đã gửi được giữ lại).
        await Wait.UntilAsync(
            async () => await CountAsync("OutboxMessage") == 0 && await CountAsync("OutboxState") == 0,
            "the delivered outbox rows have been removed",
            diagnostics: () => Api.Logs.Dump());

        Assert.Equal(1, await CountAsync("Orders"));
        Assert.Equal(1, await CountAsync("InboxState"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT COUNT(*) FROM InboxState WHERE Consumed IS NOT NULL AND Delivered IS NOT NULL"));
        Assert.Equal(1, Api.Logs.Count($"Order {orderId}: reserved 2 x SKU-001"));
        Assert.Equal(1000, await GetStockAsync("SKU-002"));
    }

    [Fact]
    public async Task TransportDown_KeepsTheMessageInTheOutbox_AndDeliversItOnceTheTransportIsBack()
    {
        Api.Transport.SetDown(true);

        // Có outbox thì transport sập cũng không làm request hỏng (đối chứng với dual-write ngây thơ).
        await PostOrderAsync("SKU-001", 4);

        await Wait.UntilAsync(
            () => Task.FromResult(Api.Logs.Count("Outbox Send Fault") >= 2),
            "the delivery service has failed (and retried) at least twice",
            diagnostics: () => Api.Logs.Dump());

        // Vẫn nằm nguyên trong database, chưa có tác dụng nào lên tồn kho.
        Assert.Equal(1, await CountAsync("OutboxMessage"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT COUNT(*) FROM OutboxState WHERE Delivered IS NULL"));
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));
        Assert.Equal(0, await CountAsync("InboxState"));

        Api.Transport.SetUp();

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-001") == 996 && await CountAsync("OutboxMessage") == 0,
            "the message was redelivered after the transport recovered",
            timeout: TimeSpan.FromSeconds(30),
            diagnostics: () => Api.Logs.Dump());

        Assert.Equal(1, await CountAsync("InboxState"));
        Assert.Equal(1, Api.Logs.Count(": reserved 4 x SKU-001"));
    }

    [Fact]
    public async Task WhileTheTransportHangs_TheDeliveryServiceKeepsDatabaseLocksOnTheOutboxRows()
    {
        const string outboxLocks = """
            SELECT COUNT(*) FROM sys.dm_tran_locks AS l
            JOIN sys.partitions AS p ON p.hobt_id = l.resource_associated_entity_id
            WHERE l.resource_database_id = DB_ID()
              AND l.resource_type IN ('KEY', 'RID')
              AND l.request_mode IN ('U', 'X')
              AND l.request_status = 'GRANT'
              AND OBJECT_NAME(p.object_id) IN (N'OutboxState', N'OutboxMessage')
            """;

        Api.Transport.Hang();

        await PostOrderAsync("SKU-001", 1);

        // Dịch vụ giao outbox đang đứng chờ transport: nó khoá dòng OutboxState (UPDLOCK) và giữ khoá đó trong suốt lúc chờ.
        await Wait.UntilAsync(
            async () => await Db.ScalarAsync<int>(outboxLocks) > 0,
            "the delivery service is blocked inside the send while holding row locks on the outbox tables",
            diagnostics: () => Api.Logs.Dump());

        Assert.Equal(1, await CountAsync("OutboxMessage"));
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));

        Api.Transport.SetUp();

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-001") == 999 && await Db.ScalarAsync<int>(outboxLocks) == 0,
            "the delivery completed and the locks were released",
            diagnostics: () => Api.Logs.Dump());
    }

    [Fact]
    public async Task ATransportThatNeverAnswers_IsCutOffByTheMessageDeliveryTimeout_AndTheMessageIsKept()
    {
        // MessageDeliveryTimeout = 1 giây trong test (mặc định 10 giây): lần gửi bị treo bị huỷ và coi là lỗi.
        Api.Transport.Hang();

        await PostOrderAsync("SKU-001", 1);

        // MassTransit ghi riêng loại lỗi này: "Outbox Send Timeout after 00:00:01 ..." (mức Warning), rồi thử lại ở lần quét sau.
        await Wait.UntilAsync(
            () => Task.FromResult(Api.Logs.Count("Outbox Send Timeout") >= 2),
            "the hanging send was cancelled by the message delivery timeout (twice: it keeps retrying)",
            timeout: TimeSpan.FromSeconds(15),
            diagnostics: () => Api.Logs.Dump());

        // Lỗi chứ không phải mất: dòng outbox vẫn nằm đó, tồn kho chưa đổi; transport hồi phục thì được giao.
        Assert.Equal(1, await CountAsync("OutboxMessage"));
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));

        Api.Transport.SetUp();

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-001") == 999 && await CountAsync("OutboxMessage") == 0,
            "the message was delivered after the transport answered again",
            diagnostics: () => Api.Logs.Dump());
    }

    [Fact]
    public async Task ConsumerFailsTwice_IsRetried_AndTheStockIsDeductedOnce()
    {
        Api.Faults.Arm(2);

        await PostOrderAsync("SKU-003", 7);

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-003") == 993,
            "the consumer succeeded on the third attempt",
            diagnostics: () => Api.Logs.Dump());

        // Hai lần lỗi đã rollback hết (kể cả InboxState) nên tác dụng nghiệp vụ chỉ xuất hiện đúng một lần.
        Assert.Equal(0, Api.Faults.Remaining);
        Assert.Equal(1, Api.Logs.Count(": reserved 7 x SKU-003"));
        Assert.True(Api.Logs.Count("R-RETRY") >= 1);
        Assert.Equal(0, Api.Logs.Count("R-FAULT"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT COUNT(*) FROM InboxState WHERE Consumed IS NOT NULL"));
    }

    [Fact]
    public async Task ConsumerFailsBeyondTheRetries_FaultsTheMessage_WithoutTouchingTheStock()
    {
        Api.Faults.Arm(100);

        await PostOrderAsync("SKU-003", 1);

        await Wait.UntilAsync(
            () => Task.FromResult(Api.Logs.Count("R-FAULT") >= 1),
            "MassTransit gave up on the message (moved it to the _error queue)",
            diagnostics: () => Api.Logs.Dump());

        // 3 khoảng retry => consumer được gọi 1 + 3 = 4 lần, rồi message bị đẩy sang hàng _error (in-memory: mất khi tắt process).
        var failures = Api.Faults.Failures;
        Assert.True(
            failures.Count == 4 && failures.Select(f => f.MessageId).Distinct().Count() == 1,
            "expected exactly 4 failed attempts of ONE message but got: " +
            string.Join(", ", failures.Select(f => $"{f.MessageId} @ {f.AtUtc:HH:mm:ss.fff}")) + "\n" + Api.Logs.Dump(150));
        Assert.Equal(100 - 4, Api.Faults.Remaining);
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-003"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT COUNT(*) FROM InboxState WHERE Consumed IS NOT NULL"));
        Assert.Equal(0, await CountAsync("OutboxMessage")); // phía producer đã giao xong; lỗi nằm ở phía consumer
    }

    [Fact]
    public async Task TheSameMessageDeliveredSeveralTimes_DeductsTheStockOnlyOnce()
    {
        const int copies = 5;

        // Một order, nhưng CÙNG MỘT message (cùng MessageId) được đưa vào outbox 5 lần, như khi broker giao lại.
        var response = await Api.CreateClient().PostAsync(
            $"/demo/orders-duplicate?copies={copies}",
            JsonContent.Create(new { sku = "SKU-002", quantity = 5, customerEmail = "buyer@example.com" }));
        response.EnsureSuccessStatusCode();

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-002") == 995,
            "the consumer applied the message",
            diagnostics: () => Api.Logs.Dump());

        // Bằng chứng các bản trùng thật sự tới consumer: ReceiveCount của dòng InboxState đếm cả chúng.
        await Wait.UntilAsync(
            async () => await Db.ScalarAsync<int>("SELECT MAX(ReceiveCount) FROM InboxState") >= copies,
            "the duplicates reached the consumer and were counted by the inbox",
            diagnostics: () => Api.Logs.Dump());
        await Task.Delay(500); // để mọi bản trùng còn lại (nếu có) được xử lý nốt

        Assert.Equal(1, await CountAsync("InboxState"));
        Assert.Equal(995, await GetStockAsync("SKU-002")); // trừ 5 một lần, không phải 25
        Assert.Equal(1, Api.Logs.Count(": reserved 5 x SKU-002"));
    }
}
