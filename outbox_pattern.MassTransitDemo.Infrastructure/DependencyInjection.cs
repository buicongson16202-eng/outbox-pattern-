using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using outbox_pattern.MassTransitDemo.Application.Abstractions;
using outbox_pattern.MassTransitDemo.Infrastructure.Demo;
using outbox_pattern.MassTransitDemo.Infrastructure.Messaging;
using outbox_pattern.MassTransitDemo.Infrastructure.Persistence;

namespace outbox_pattern.MassTransitDemo.Infrastructure
{
    public static class DependencyInjection
    {
        /// <summary>Cài đặt các cổng của Application (EF Core + MassTransit) và cấu hình outbox/inbox, transport.</summary>
        public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
        {
            var settings = configuration.GetSection(OutboxSettings.SectionName).Get<OutboxSettings>() ?? new OutboxSettings();

            // --- Persistence: một DbContext cho cả dữ liệu nghiệp vụ lẫn bảng outbox/inbox ---
            services.AddDbContext<AppDbContext>(options =>
                options.UseSqlServer(configuration.GetConnectionString("Default")));

            services.AddScoped<IUnitOfWork>(sp => sp.GetRequiredService<AppDbContext>());
            services.AddScoped<IOrderRepository, OrderRepository>();
            services.AddScoped<IStockRepository, StockRepository>();
            services.AddScoped<IOrderEventPublisher, MassTransitOrderEventPublisher>();

            // --- Công cụ demo (xem DemoController): bắt consumer lỗi, giả lập transport sập/treo, đọc trạng thái ---
            services.AddSingleton<ConsumerFaults>();
            services.AddSingleton<TransportSwitch>();
            services.AddScoped<DemoTools>();

            // --- Outbox pattern bằng MassTransit ---
            services.AddMassTransit(x =>
            {
                // (1) Outbox phía producer + inbox phía consumer, lưu bằng EF Core trong CHÍNH database nghiệp vụ.
                //     Thay cho: bảng OutboxMessages tự viết + OutboxProcessor + InboxMessages + MessageCleanupService.
                x.AddEntityFrameworkOutbox<AppDbContext>(outbox =>
                {
                    outbox.UseSqlServer();   // chọn cách khoá hàng của SQL Server (UPDLOCK/ROWLOCK) để các instance chia việc
                    outbox.UseBusOutbox(bus =>
                    {
                        // IPublishEndpoint/ISendEndpointProvider sẽ ghi vào bảng OutboxMessage thay vì gửi thẳng.
                        bus.MessageDeliveryLimit = settings.MessageDeliveryLimit;     // số message tối đa mỗi lần giao (như BatchSize)
                        bus.MessageDeliveryTimeout = settings.MessageDeliveryTimeout; // thời gian tối đa cho MỘT lần gửi (như PublishTimeout)
                    });

                    outbox.QueryDelay = settings.QueryDelay;
                    outbox.DuplicateDetectionWindow = settings.DuplicateDetectionWindow;
                });

                x.AddConsumer<OrderCreatedConsumer>();

                // (2) Áp dụng cho MỌI receive endpoint. Thứ tự middleware quan trọng: retry đặt TRƯỚC outbox để mỗi lần thử lại
                //     chạy trọn một transaction mới (consumer lỗi → rollback hết, kể cả InboxState → thử lại từ đầu).
                x.AddConfigureEndpointsCallback((context, name, endpoint) =>
                {
                    if (settings.ConsumerRetryIntervals.Length > 0)
                    {
                        endpoint.UseMessageRetry(retry => retry.Intervals(settings.ConsumerRetryIntervals));
                    }

                    endpoint.UseEntityFrameworkOutbox<AppDbContext>(context);
                });

                // (3) Transport. In-memory để demo chạy không cần hạ tầng; mất sạch khi process tắt.
                //     Đổi sang RabbitMQ/Azure Service Bus chỉ đổi phần này (xem docs/masstransit.md mục 8).
                x.UsingInMemory((context, cfg) =>
                {
                    // Demo: công tắc Down làm mọi lần GỬI ra transport ném lỗi (giả lập broker sập), công tắc Hang làm chúng treo
                    // (broker không trả lời). Dịch vụ giao outbox đẩy từng dòng OutboxMessage ra transport bằng Send nên sẽ thất bại
                    // và thử lại (Down) hoặc đứng chờ (Hang); dòng đó vẫn nằm nguyên trong database. Cố ý KHÔNG chặn pipeline Publish:
                    // IPublishEndpoint trong request chỉ ghi vào outbox, nhưng vẫn chạy qua pipeline Publish để dựng message, nên
                    // chặn ở đó sẽ làm hỏng cả POST /orders.
                    var transport = context.GetRequiredService<TransportSwitch>();
                    cfg.ConfigureSend(send => send.UseExecuteAsync(sendContext => transport.BeforeSendAsync(sendContext.CancellationToken)));

                    cfg.ConfigureEndpoints(context);
                });
            });

            return services;
        }

        /// <summary>
        /// Demo: tự tạo database và bảng (kể cả InboxState/OutboxMessage/OutboxState) nếu chưa có. Production nên dùng EF migrations.
        /// </summary>
        public static void EnsureDatabaseCreated(this IServiceProvider services)
        {
            using var scope = services.CreateScope();
            scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreated();
        }
    }
}
