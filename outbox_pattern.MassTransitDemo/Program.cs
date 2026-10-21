using MassTransit;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Inventory;
using outbox_pattern.MassTransitDemo.Messaging;
using outbox_pattern.MassTransitDemo.Persistence;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

var settings = builder.Configuration.GetSection(OutboxSettings.SectionName).Get<OutboxSettings>() ?? new OutboxSettings();

// --- Outbox pattern bằng MassTransit ---
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("Default")));

// Hai công tắc demo (xem DemoController): bắt consumer lỗi, và giả lập transport sập.
builder.Services.AddSingleton<ConsumerFaults>();
builder.Services.AddSingleton<TransportSwitch>();

builder.Services.AddMassTransit(x =>
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
    //     Đổi sang RabbitMQ/Azure Service Bus chỉ đổi dòng này (xem docs/masstransit.md mục 8).
    x.UsingInMemory((context, cfg) =>
    {
        // Demo: công tắc Down làm mọi lần GỬI ra transport ném lỗi (giả lập broker sập), công tắc Hang làm chúng treo (broker không
        // trả lời). Dịch vụ giao outbox đẩy từng dòng OutboxMessage ra transport bằng Send nên sẽ thất bại và thử lại (Down) hoặc
        // đứng chờ (Hang); dòng đó vẫn nằm nguyên trong database. Cố ý KHÔNG chặn pipeline Publish: IPublishEndpoint trong
        // controller chỉ ghi vào outbox, nhưng vẫn chạy qua pipeline Publish để dựng message, nên chặn ở đó sẽ làm hỏng cả POST /orders.
        var transport = context.GetRequiredService<TransportSwitch>();
        cfg.ConfigureSend(send => send.UseExecuteAsync(sendContext => transport.BeforeSendAsync(sendContext.CancellationToken)));

        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();

// Demo: tự tạo database và bảng (kể cả InboxState/OutboxMessage/OutboxState) nếu chưa có. Production nên dùng EF migrations.
using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreated();
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();

    // Swagger UI đọc tài liệu OpenAPI do MapOpenApi() ở trên sinh ra (/openapi/v1.json); giao diện tại /swagger.
    app.UseSwaggerUI(options => options.SwaggerEndpoint("/openapi/v1.json", "outbox_pattern.MassTransitDemo v1"));
}
else
{
    // Ở Development không chuyển hướng sang HTTPS để Swagger dùng được cả trên http:// lẫn https:// (khác cổng = khác origin).
    app.UseHttpsRedirection();
}

app.UseAuthorization();

app.MapControllers();

app.Run();
