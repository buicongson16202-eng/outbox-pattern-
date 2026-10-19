using System.Net;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.Contracts;
using outbox_pattern.Messaging;
using outbox_pattern.Tests.Infrastructure;

namespace outbox_pattern.Tests;

/// <summary>Kịch bản 1: POST /orders ghi Order + OutboxMessage nguyên tử trong một transaction (docs mục 4.1, 3.1).</summary>
[Collection(LocalDbCollection.Name)]
public class OrdersApiTests(LocalDbFixture db) : DbTestBase(db)
{
    [Fact]
    public async Task PostOrder_WritesTheOrderAndAPendingOutboxMessage()
    {
        var response = await PostOrderRawAsync(new { sku = "SKU-001", quantity = 3, customerEmail = "buyer@example.com" });

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        await using var context = Db.CreateDbContext();
        var order = await context.Orders.AsNoTracking().SingleAsync();
        var message = await context.OutboxMessages.AsNoTracking().SingleAsync();

        Assert.Equal("SKU-001", order.Sku);
        Assert.Equal(3, order.Quantity);

        Assert.Equal(OrderCreatedEvent.TypeName, message.Type);
        Assert.Equal(order.Id.ToString(), message.AggregateId);
        Assert.Null(message.ProcessedOnUtc);
        Assert.Equal(0, message.Attempts);

        var payload = JsonSerializer.Deserialize<OrderCreatedEvent>(message.Payload);
        Assert.NotNull(payload);
        Assert.Equal(order.Id, payload.OrderId);
        Assert.Equal("SKU-001", payload.Sku);
        Assert.Equal(3, payload.Quantity);
        Assert.Equal("buyer@example.com", payload.CustomerEmail);

        // Controller không đụng tới broker hay tồn kho: việc đó thuộc về relay và consumer, sau commit.
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));
        Assert.Equal(0, await CountInboxAsync());
    }

    [Fact]
    public async Task PostOrder_WhenSaveChangesFailsAfterTheOrderRowWasWritten_LeavesNeitherOrderNorOutboxMessage()
    {
        // Gây lỗi ở INSERT thứ hai của cùng SaveChanges: trigger chỉ ném khi dòng Orders tương ứng ĐÃ được ghi
        // trong transaction hiện tại. Vì vậy nếu EF ghi outbox trước order (hoặc dùng hai transaction), trigger không
        // ném → API trả 201 → test fail ồn ào, thay vì pass một cách "rỗng".
        await Db.ExecuteAsync("""
            CREATE TRIGGER dbo.TR_Test_FailOutboxInsert ON dbo.OutboxMessages AFTER INSERT AS
            BEGIN
                IF EXISTS (SELECT 1 FROM inserted AS i
                           JOIN dbo.Orders AS o ON o.Id = TRY_CONVERT(uniqueidentifier, i.AggregateId))
                    THROW 50001, 'Simulated failure after the Order row was written', 1;
            END
            """);

        try
        {
            var response = await PostOrderRawAsync(new { sku = "SKU-001", quantity = 3, customerEmail = "buyer@example.com" });

            Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        }
        finally
        {
            await Db.ExecuteAsync("DROP TRIGGER dbo.TR_Test_FailOutboxInsert");
        }

        await using var context = Db.CreateDbContext();
        Assert.Empty(await context.Orders.AsNoTracking().ToListAsync());
        Assert.Empty(await context.OutboxMessages.AsNoTracking().ToListAsync());
    }

    [Theory]
    [InlineData(0, "buyer@example.com")]      // Quantity ngoài [1, 1000]
    [InlineData(5, "not-an-email")]           // CustomerEmail sai định dạng
    public async Task PostOrder_WithAnInvalidBody_ReturnsBadRequestAndWritesNothing(int quantity, string email)
    {
        var response = await PostOrderRawAsync(new { sku = "SKU-001", quantity, customerEmail = email });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        await using var context = Db.CreateDbContext();
        Assert.Empty(await context.Orders.AsNoTracking().ToListAsync());
        Assert.Empty(await context.OutboxMessages.AsNoTracking().ToListAsync());
    }
}
