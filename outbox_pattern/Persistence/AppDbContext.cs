using Microsoft.EntityFrameworkCore;
using outbox_pattern.Domain;
using outbox_pattern.Messaging;

namespace outbox_pattern.Persistence
{
    public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        public DbSet<StockItem> StockItems => Set<StockItem>();

        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

        public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();

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

            modelBuilder.Entity<OutboxMessage>(outbox =>
            {
                // Tên bảng được viết cứng vì OutboxProcessor truy vấn bằng SQL thô.
                outbox.ToTable("OutboxMessages");
                outbox.HasKey(m => m.Id);
                outbox.Property(m => m.Id).ValueGeneratedNever();
                outbox.Property(m => m.Type).HasMaxLength(200);
                outbox.Property(m => m.AggregateId).HasMaxLength(100);
                outbox.Property(m => m.LastError).HasMaxLength(2000);

                // Phục vụ đúng câu truy vấn "nóng" của relay. Là filtered index nên chỉ chứa các dòng CHƯA xử lý:
                // luôn nhỏ dù bảng có hàng triệu dòng lịch sử.
                outbox.HasIndex(m => new { m.NextAttemptOnUtc, m.OccurredOnUtc })
                    .HasDatabaseName("IX_OutboxMessages_Pending")
                    .HasFilter("[ProcessedOnUtc] IS NULL");

                // Phục vụ job dọn dẹp (tìm các dòng đã xử lý và quá hạn).
                outbox.HasIndex(m => m.ProcessedOnUtc)
                    .HasDatabaseName("IX_OutboxMessages_Processed")
                    .HasFilter("[ProcessedOnUtc] IS NOT NULL");
            });

            modelBuilder.Entity<InboxMessage>(inbox =>
            {
                inbox.ToTable("InboxMessages");
                inbox.HasKey(m => new { m.MessageId, m.Consumer });
                inbox.Property(m => m.Consumer).HasMaxLength(200);
                inbox.HasIndex(m => m.ProcessedOnUtc);
            });
        }
    }
}
