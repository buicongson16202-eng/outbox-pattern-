using System.Net;
using System.Net.Http.Json;
using outbox_pattern.MassTransitDemo.Controllers;

namespace outbox_pattern.MassTransitDemo.Tests.Infrastructure;

/// <summary>
/// Nền chung cho các test dùng database tạm. Mỗi test có một instance ứng dụng RIÊNG (bus, transport in-memory và các dịch vụ
/// nền mới tinh) trên một database vừa được dọn sạch, nên không có message còn sót lại của test trước làm nhiễu test sau
/// (bản trùng của test trước có thể vẫn đang được retry). Lớp con vẫn phải tự gắn [Collection(LocalDbCollection.Name)].
/// </summary>
public abstract class DbTestBase(LocalDbFixture db) : IAsyncLifetime
{
    protected LocalDbFixture Db { get; } = db;

    /// <summary>Instance ứng dụng của test hiện tại.</summary>
    protected DemoApiFactory Api { get; private set; } = null!;

    public virtual async Task InitializeAsync()
    {
        await Db.ResetAsync();
        Api = Db.CreateApi();
    }

    public virtual Task DisposeAsync()
    {
        Api.Dispose(); // dừng bus và các dịch vụ nền trước khi test kế tiếp dọn database
        return Task.CompletedTask;
    }

    protected Task<HttpResponseMessage> PostOrderRawAsync(object body, DemoApiFactory? api = null) =>
        (api ?? Api).CreateClient().PostAsJsonAsync("/orders", body);

    /// <summary>POST /orders (qua HTTP, đúng controller của project) và trả về Id của order vừa tạo.</summary>
    protected async Task<Guid> PostOrderAsync(string sku, int quantity, DemoApiFactory? api = null)
    {
        var response = await PostOrderRawAsync(new { sku, quantity, customerEmail = "buyer@example.com" }, api);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        var order = await response.Content.ReadFromJsonAsync<OrderResponse>();
        Assert.NotNull(order);

        return order.Id;
    }

    protected Task<int> CountAsync(string table) => Db.ScalarAsync<int>($"SELECT COUNT(*) FROM {table}");

    protected Task<int> GetStockAsync(string sku) =>
        Db.ScalarAsync<int>($"SELECT Available FROM StockItems WHERE Sku = N'{sku}'");
}
