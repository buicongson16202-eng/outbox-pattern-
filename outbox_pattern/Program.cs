using Microsoft.EntityFrameworkCore;
using outbox_pattern.Inventory;
using outbox_pattern.Messaging;
using outbox_pattern.Persistence;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// --- Outbox pattern ---
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("Default")));

builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection(OutboxOptions.SectionName));

// Broker giả lập trong bộ nhớ. Ở production: viết một IMessagePublisher cho RabbitMQ/Kafka/Service Bus
// và một consumer tương ứng — outbox, relay và inbox giữ nguyên.
builder.Services.AddSingleton<InMemoryBroker>();
builder.Services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<InMemoryBroker>());

builder.Services.AddHostedService<OutboxProcessor>();       // relay: outbox -> broker
builder.Services.AddHostedService<MessageCleanupService>(); // dọn outbox/inbox đã cũ
builder.Services.AddHostedService<InventoryConsumer>();     // consumer: broker -> inbox -> tồn kho

var app = builder.Build();

// Demo: tự tạo database và bảng nếu chưa có. Production nên dùng EF migrations.
// Nếu chạy nhiều instance, hãy khởi động instance đầu tiên xong rồi mới bật các instance còn lại.
using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreated();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();

    // Swagger UI đọc tài liệu OpenAPI do MapOpenApi() ở trên sinh ra (/openapi/v1.json); giao diện tại /swagger.
    app.UseSwaggerUI(options => options.SwaggerEndpoint("/openapi/v1.json", "outbox_pattern v1"));
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
