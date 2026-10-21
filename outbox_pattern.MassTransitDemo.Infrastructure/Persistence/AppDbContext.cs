using MassTransit;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Domain.Inventory;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Infrastructure.Persistence
{
    /// <summary>
    /// Một DbContext cho cả dữ liệu nghiệp vụ lẫn ba bảng của MassTransit — điều kiện bắt buộc để Order và message nằm trong
    /// CÙNG một transaction. Nó cũng chính là <see cref="IUnitOfWork"/> của Application (SaveChangesAsync = commit cả hai).
    /// </summary>
    public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options), IUnitOfWork
    {
        public DbSet<Order> Orders => Set<Order>();

        public DbSet<StockItem> StockItems => Set<StockItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Order>(order =>
            {
                order.ToTable("Orders");
                order.HasKey(o => o.Id);
                order.Property(o => o.Id).ValueGeneratedNever();
                order.Property(o => o.Sku).HasMaxLength(Order.MaxSkuLength);
                order.Property(o => o.CustomerEmail).HasMaxLength(Order.MaxEmailLength);
            });

            modelBuilder.Entity<StockItem>(stock =>
            {
                stock.ToTable("StockItems");
                stock.HasKey(s => s.Sku);
                stock.Property(s => s.Sku).HasMaxLength(Order.MaxSkuLength);
                stock.HasData(
                    new StockItem("SKU-001", 1000),
                    new StockItem("SKU-002", 1000),
                    new StockItem("SKU-003", 1000));
            });

            // Ba bảng do MassTransit sở hữu (không cần tự viết entity/relay/inbox):
            //   OutboxMessage - hàng đợi các message chờ gửi, ghi cùng transaction với dữ liệu nghiệp vụ
            //   OutboxState   - theo dõi việc giao từng "lô" outbox, đồng thời là chỗ các instance tranh nhau bằng row-lock
            //   InboxState    - sổ "consumer nào đã nhận message nào", dùng để khử trùng lặp
            modelBuilder.AddInboxStateEntity();
            modelBuilder.AddOutboxMessageEntity();
            modelBuilder.AddOutboxStateEntity();
        }
    }
}
