using System.Net;
using outbox_pattern.MassTransitDemo.Tests.Infrastructure;

namespace outbox_pattern.MassTransitDemo.Tests;

/// <summary>POST /orders ghi Order + dòng outbox của MassTransit nguyên tử trong một transaction (docs/masstransit.md mục 4.1).</summary>
[Collection(LocalDbCollection.Name)]
public class OrdersOutboxTests(LocalDbFixture db) : DbTestBase(db)
{
    [Fact]
    public async Task PostOrder_WritesTheOrderAndOneUndeliveredOutboxMessage()
    {
        // Transport "sập" để dòng outbox không bị giao đi trước khi test kịp nhìn thấy nó.
        Api.Transport.SetDown(true);

        var orderId = await PostOrderAsync("SKU-001", 3);

        Assert.Equal(1, await CountAsync("Orders"));
        Assert.Equal(1, await CountAsync("OutboxMessage"));
        Assert.Equal(1, await CountAsync("OutboxState"));

        Assert.Equal(
            "urn:message:orders.order-created.v1",
            await Db.ScalarAsync<string>("SELECT MessageType FROM OutboxMessage"));
        Assert.Contains(
            orderId.ToString(),
            await Db.ScalarAsync<string>("SELECT Body FROM OutboxMessage"),
            StringComparison.OrdinalIgnoreCase);

        // Chưa giao: OutboxState.Delivered còn NULL, controller không đụng tới tồn kho, consumer chưa nhận gì.
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT COUNT(*) FROM OutboxState WHERE Delivered IS NULL"));
        Assert.Equal(LocalDbFixture.InitialStock, await GetStockAsync("SKU-001"));
        Assert.Equal(0, await CountAsync("InboxState"));
    }

    [Fact]
    public async Task PostOrder_WhenTheOutboxInsertFailsAfterTheOrderWasWritten_LeavesNeitherOrderNorOutboxRows()
    {
        // Gây lỗi ở việc ghi OutboxMessage bằng một CHECK constraint luôn sai (dùng constraint, không dùng trigger: câu INSERT
        // của MassTransit có mệnh đề OUTPUT nên SQL Server cấm bảng này có trigger — lỗi 334).
        await Db.ExecuteAsync("ALTER TABLE dbo.OutboxMessage ADD CONSTRAINT CK_Test_FailOutboxInsert CHECK (1 = 0)");

        // Log câu lệnh SQL để chứng minh lỗi xảy ra SAU khi INSERT Orders đã chạy trong cùng transaction — nếu không, việc
        // "không còn gì" chỉ là vì chưa có gì được ghi và test sẽ pass một cách rỗng.
        using var api = Db.CreateApi(logSql: true);

        try
        {
            var response = await PostOrderRawAsync(
                new { sku = "SKU-001", quantity = 3, customerEmail = "buyer@example.com" }, api);

            Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        }
        finally
        {
            await Db.ExecuteAsync("ALTER TABLE dbo.OutboxMessage DROP CONSTRAINT CK_Test_FailOutboxInsert");
        }

        Assert.True(api.Logs.HasExceptionContaining("CK_Test_FailOutboxInsert"), "the failure must come from the outbox insert\n" + api.Logs.Dump());

        var sql = api.Logs.Entries.Where(e => e.Category == "Microsoft.EntityFrameworkCore.Database.Command").Select(e => e.Message).ToList();
        var orderInsertRan = sql.Any(m => m.StartsWith("Executed DbCommand", StringComparison.Ordinal) && m.Contains("INSERT INTO [Orders]", StringComparison.Ordinal))
            || sql.Any(m => m.StartsWith("Failed executing DbCommand", StringComparison.Ordinal)
                            && m.IndexOf("INSERT INTO [Orders]", StringComparison.Ordinal) is var o and >= 0
                            && m.IndexOf("INSERT INTO [OutboxMessage]", StringComparison.Ordinal) is var m2 && m2 > o);
        Assert.True(orderInsertRan, "INSERT INTO [Orders] must have run before the failing INSERT INTO [OutboxMessage]\n" + string.Join("\n---\n", sql));

        Assert.Equal(0, await CountAsync("Orders"));
        Assert.Equal(0, await CountAsync("OutboxMessage"));
        Assert.Equal(0, await CountAsync("OutboxState"));
    }

    [Theory]
    [InlineData(0, "buyer@example.com")]      // Quantity ngoài [1, 1000]
    [InlineData(5, "not-an-email")]           // CustomerEmail sai định dạng
    public async Task PostOrder_WithAnInvalidBody_ReturnsBadRequestAndWritesNothing(int quantity, string email)
    {
        var response = await PostOrderRawAsync(new { sku = "SKU-001", quantity, customerEmail = email });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await CountAsync("Orders"));
        Assert.Equal(0, await CountAsync("OutboxMessage"));
        Assert.Equal(0, await CountAsync("OutboxState"));
    }
}
