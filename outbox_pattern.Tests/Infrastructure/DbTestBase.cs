using System.Net;
using System.Net.Http.Json;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.Controllers;
using outbox_pattern.Messaging;

namespace outbox_pattern.Tests.Infrastructure;

/// <summary>
/// Nền chung cho các test dùng database tạm: reset dữ liệu trước mỗi test và vài hàm đọc/ghi hay dùng.
/// Lớp con vẫn phải tự gắn [Collection(LocalDbCollection.Name)].
/// </summary>
public abstract class DbTestBase(LocalDbFixture db) : IAsyncLifetime
{
    protected LocalDbFixture Db { get; } = db;

    public virtual Task InitializeAsync() => Db.ResetAsync();

    public virtual Task DisposeAsync() => Task.CompletedTask;

    protected Task<HttpResponseMessage> PostOrderRawAsync(object body) =>
        Db.Api.CreateClient().PostAsJsonAsync("/orders", body);

    /// <summary>POST /orders (qua HTTP, đúng controller production) và trả về Id của order cùng Id của dòng outbox sinh ra.</summary>
    protected async Task<(Guid OrderId, Guid MessageId)> PostOrderAsync(string sku, int quantity)
    {
        var response = await PostOrderRawAsync(new { sku, quantity, customerEmail = "buyer@example.com" });
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var order = await response.Content.ReadFromJsonAsync<OrderResponse>();
        Assert.NotNull(order);

        await using var context = Db.CreateDbContext();
        var orderId = order.Id.ToString();
        var messageId = await context.OutboxMessages
            .AsNoTracking()
            .Where(m => m.AggregateId == orderId)
            .Select(m => m.Id)
            .SingleAsync();

        return (order.Id, messageId);
    }

    protected async Task<OutboxMessage> GetOutboxAsync(Guid messageId)
    {
        await using var context = Db.CreateDbContext();
        return await context.OutboxMessages.AsNoTracking().SingleAsync(m => m.Id == messageId);
    }

    protected async Task<int> GetStockAsync(string sku)
    {
        await using var context = Db.CreateDbContext();
        return await context.StockItems.AsNoTracking().Where(s => s.Sku == sku).Select(s => s.Available).SingleAsync();
    }

    protected async Task<int> CountInboxAsync(Guid? messageId = null)
    {
        await using var context = Db.CreateDbContext();
        var query = context.InboxMessages.AsNoTracking();

        return messageId is { } id
            ? await query.CountAsync(m => m.MessageId == id)
            : await query.CountAsync();
    }
}
