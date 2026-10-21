using outbox_pattern.MassTransitDemo.Application;
using outbox_pattern.MassTransitDemo.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Composition root: nối các tầng của Clean Architecture.
//   Application    - use case (CreateOrderHandler, ReserveStockHandler…) và các cổng (interface)
//   Infrastructure - cài đặt các cổng bằng EF Core + MassTransit (outbox/inbox, transport, consumer)
builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);

var app = builder.Build();

// Demo: tự tạo database và bảng (kể cả InboxState/OutboxMessage/OutboxState) nếu chưa có. Production nên dùng EF migrations.
app.Services.EnsureDatabaseCreated();

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
