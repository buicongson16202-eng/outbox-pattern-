using outbox_pattern.MassTransitDemo.Tests.Infrastructure;

namespace outbox_pattern.MassTransitDemo.Tests;

/// <summary>
/// Nhiều instance của ứng dụng dùng chung một database (docs/masstransit.md mục 9). Mỗi instance là một host đầy đủ
/// (dịch vụ giao outbox + consumer riêng, transport in-memory riêng); chúng chỉ phối hợp với nhau qua database.
/// </summary>
[Collection(LocalDbCollection.Name)]
public class ScaleOutTests(LocalDbFixture db) : DbTestBase(db)
{
    [Fact]
    public async Task AnotherInstance_TakesOverTheOutboxRows_WhenThisInstanceCannotReachTheTransport()
    {
        using var other = Db.CreateApi();
        other.Logs.Clear();

        // Instance chính không với tới transport (như bị đứt mạng); instance còn lại thì bình thường.
        Api.Transport.SetDown(true);

        for (var i = 0; i < 10; i++)
        {
            await PostOrderAsync("SKU-001", 1); // toàn bộ order đi qua instance CHÍNH
        }

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-001") == LocalDbFixture.InitialStock - 10,
            "the other instance delivered all ten messages and its consumer applied them",
            timeout: TimeSpan.FromSeconds(60),
            diagnostics: () => "-- main --\n" + Api.Logs.Dump(30) + "\n-- other --\n" + other.Logs.Dump(30));

        // Việc giao do instance kia làm, và consumer của NÓ nhận message (mỗi instance có transport in-memory riêng).
        Assert.Equal(10, other.Logs.Count(": reserved 1 x SKU-001"));
        Assert.Equal(0, Api.Logs.Count(": reserved"));
        Assert.Equal(10, await CountAsync("InboxState"));
        Assert.Equal(0, await CountAsync("OutboxMessage"));
    }

    [Fact]
    public async Task TwoHealthyInstances_ApplyEveryOrderExactlyOnce()
    {
        const int orders = 60;

        using var other = Db.CreateApi();
        other.Logs.Clear();

        // Gửi order qua cả hai instance cùng lúc để hai dịch vụ giao outbox thật sự tranh nhau.
        await Task.WhenAll(Enumerable.Range(0, orders).Select(i => PostOrderAsync("SKU-002", 1, i % 2 == 0 ? Api : other)));

        await Wait.UntilAsync(
            async () => await GetStockAsync("SKU-002") == LocalDbFixture.InitialStock - orders && await CountAsync("OutboxMessage") == 0,
            $"all {orders} orders were delivered and applied",
            timeout: TimeSpan.FromSeconds(60),
            diagnostics: () => "-- main --\n" + Api.Logs.Dump(30) + "\n-- other --\n" + other.Logs.Dump(30));

        await Task.Delay(500); // thời gian cho bản trùng (nếu có) tới nơi

        var appliedByMain = Api.Logs.Count(": reserved 1 x SKU-002");
        var appliedByOther = other.Logs.Count(": reserved 1 x SKU-002");

        Assert.Equal(orders, appliedByMain + appliedByOther);
        Assert.Equal(orders, await CountAsync("InboxState"));
        Assert.Equal(LocalDbFixture.InitialStock - orders, await GetStockAsync("SKU-002"));
    }
}
