using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Persistence
{
    public class OrderRepository(AppDbContext db) : IOrderRepository
    {
        public void Add(Order order) => db.Orders.Add(order);

        public Task<Order?> GetByIdAsync(Guid id, CancellationToken cancellationToken) =>
            db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id, cancellationToken);
    }

    public class StockRepository(AppDbContext db) : IStockRepository
    {
        /// <summary>
        /// Một câu UPDATE có điều kiện: kiểm tra "còn đủ hàng" và trừ kho là MỘT thao tác nguyên tử trong database, nên nhiều
        /// consumer trừ cùng SKU đồng thời vẫn đúng. Câu lệnh chạy ngay trên transaction hiện tại của DbContext — khi được gọi
        /// từ consumer, đó là transaction do middleware outbox/inbox của MassTransit mở, nên dấu inbox và việc trừ kho
        /// cùng commit hoặc cùng rollback.
        /// </summary>
        public async Task<bool> TryReserveAsync(string sku, int quantity, CancellationToken cancellationToken) =>
            await db.StockItems
                .Where(s => s.Sku == sku && s.Available >= quantity)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Available, x => x.Available - quantity), cancellationToken) > 0;
    }
}
