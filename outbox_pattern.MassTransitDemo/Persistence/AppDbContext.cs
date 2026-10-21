using MassTransit;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Domain;

namespace outbox_pattern.MassTransitDemo.Persistence
{
    public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
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
                order.Property(o => o.Sku).HasMaxLength(50);
                order.Property(o => o.CustomerEmail).HasMaxLength(320);
            });

            modelBuilder.Entity<StockItem>(stock =>
            {
                stock.ToTable("StockItems");
                stock.HasKey(s => s.Sku);
                stock.Property(s => s.Sku).HasMaxLength(50);
                stock.HasData(
                    new StockItem { Sku = "SKU-001", Available = 1000 },
                    new StockItem { Sku = "SKU-002", Available = 1000 },
                    new StockItem { Sku = "SKU-003", Available = 1000 });
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
