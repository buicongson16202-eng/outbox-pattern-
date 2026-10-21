using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Application.Inventory;
using outbox_pattern.MassTransitDemo.Application.Orders;
using outbox_pattern.MassTransitDemo.Domain.Common;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Tests.Unit;

/// <summary>
/// Unit test cho Domain và Application: không database, không MassTransit, chạy trong vài mili-giây. Đây là lợi ích trực tiếp
/// của việc tách tầng — use case chỉ phụ thuộc các cổng (interface) nên thay bằng bản giả được.
/// </summary>
public class UseCaseTests
{
    private static readonly DateTime Now = new(2026, 10, 21, 8, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(0)]
    [InlineData(Order.MaxQuantity + 1)]
    public void Order_Create_RejectsAQuantityOutsideTheAllowedRange(int quantity)
    {
        Assert.Throws<DomainException>(() => Order.Create("SKU-001", quantity, "buyer@example.com", Now));
    }

    [Fact]
    public async Task CreateOrder_AddsTheOrder_PublishesOrderCreated_ThenCommitsOnce()
    {
        var journal = new List<string>();
        var handler = new CreateOrderHandler(
            new FakeOrders(journal), new FakeEvents(journal), new FakeUnitOfWork(journal), new FixedClock(Now));

        var order = await handler.HandleAsync(new CreateOrderCommand("SKU-001", 3, "buyer@example.com"), CancellationToken.None);

        // Thứ tự là điểm mấu chốt của outbox: message được "publish" vào unit of work TRƯỚC lần commit duy nhất,
        // nên order và message nằm trong cùng một transaction.
        Assert.Equal([$"add {order.Id}", $"publish OrderCreated {order.Id}", "commit"], journal);
        Assert.Equal(Now, order.CreatedOnUtc);
        Assert.Equal(3, order.Quantity);
    }

    [Fact]
    public async Task CreateOrder_RejectedByTheDomain_NeitherPublishesNorCommits()
    {
        var journal = new List<string>();
        var handler = new CreateOrderHandler(
            new FakeOrders(journal), new FakeEvents(journal), new FakeUnitOfWork(journal), new FixedClock(Now));

        await Assert.ThrowsAsync<DomainException>(() =>
            handler.HandleAsync(new CreateOrderCommand("SKU-001", 0, "buyer@example.com"), CancellationToken.None));

        Assert.Empty(journal);
    }

    [Theory]
    [InlineData(true, ReserveStockResult.Reserved)]
    [InlineData(false, ReserveStockResult.Rejected)]
    public async Task ReserveStock_TranslatesTheRepositoryOutcome(bool reserved, ReserveStockResult expected)
    {
        var handler = new ReserveStockHandler(new FakeStock(reserved));

        var result = await handler.HandleAsync(new ReserveStockCommand(Guid.NewGuid(), "SKU-001", 2), CancellationToken.None);

        Assert.Equal(expected, result);
    }

    private sealed class FakeOrders(List<string> journal) : IOrderRepository
    {
        public void Add(Order order) => journal.Add($"add {order.Id}");

        public Task<Order?> GetByIdAsync(Guid id, CancellationToken cancellationToken) => Task.FromResult<Order?>(null);
    }

    private sealed class FakeEvents(List<string> journal) : IOrderEventPublisher
    {
        public Task PublishOrderCreatedAsync(Order order, CancellationToken cancellationToken)
        {
            journal.Add($"publish OrderCreated {order.Id}");
            return Task.CompletedTask;
        }
    }

    private sealed class FakeUnitOfWork(List<string> journal) : IUnitOfWork
    {
        public Task<int> SaveChangesAsync(CancellationToken cancellationToken)
        {
            journal.Add("commit");
            return Task.FromResult(2);
        }
    }

    private sealed class FakeStock(bool reserved) : IStockRepository
    {
        public Task<bool> TryReserveAsync(string sku, int quantity, CancellationToken cancellationToken) => Task.FromResult(reserved);
    }

    private sealed class FixedClock(DateTime utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => new(utcNow);
    }
}
