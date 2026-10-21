using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using outbox_pattern.MassTransitDemo.Api.Controllers;
using outbox_pattern.MassTransitDemo.Infrastructure.Demo;
using outbox_pattern.MassTransitDemo.Infrastructure.Persistence;

namespace outbox_pattern.MassTransitDemo.Tests.Infrastructure;

/// <summary>
/// Chạy đúng Program.cs của project trong bộ nhớ — gồm cả bus MassTransit, dịch vụ giao outbox và consumer thật,
/// vì chính chúng là thứ cần kiểm thử. Mỗi factory là MỘT "instance" của ứng dụng (có transport in-memory riêng);
/// tạo hai factory trỏ vào cùng database để mô phỏng scale-out.
/// Điều chỉnh duy nhất: connection string trỏ vào database tạm, và QueryDelay ngắn để test không phải chờ lâu.
/// <see cref="OrdersController"/> chỉ dùng để chỉ ra assembly của ứng dụng (<c>Program</c> là internal).
/// </summary>
public sealed class DemoApiFactory(string connectionString, bool logSql = false) : WebApplicationFactory<OrdersController>
{
    static DemoApiFactory()
    {
        // AddInfrastructure (gọi từ Program.cs) đọc mục "Outbox" NGAY khi khởi động (trước khi factory kịp thêm nguồn cấu hình), nên các giá trị này phải
        // đi qua biến môi trường — nguồn mà WebApplication.CreateBuilder nạp sẵn. (Connection string thì được đọc muộn hơn nên
        // đặt được qua ConfigureAppConfiguration bên dưới.)
        SetIfMissing("Outbox__QueryDelay", "00:00:00.100");
        SetIfMissing("Outbox__MessageDeliveryTimeout", "00:00:01");
        SetIfMissing("Outbox__ConsumerRetryIntervals__0", "00:00:00.050");
        SetIfMissing("Outbox__ConsumerRetryIntervals__1", "00:00:00.100");
        SetIfMissing("Outbox__ConsumerRetryIntervals__2", "00:00:00.200");
    }

    private static void SetIfMissing(string name, string value)
    {
        if (Environment.GetEnvironmentVariable(name) is null)
        {
            Environment.SetEnvironmentVariable(name, value);
        }
    }

    public LogSink Logs { get; } = new();

    public TransportSwitch Transport => Services.GetRequiredService<TransportSwitch>();

    public ConsumerFaults Faults => Services.GetRequiredService<ConsumerFaults>();

    /// <summary>Chốt chặn an toàn: tuyệt đối không để test ghi nhầm vào database demo trong appsettings.json.</summary>
    public void AssertConnectedTo(string expectedDatabase)
    {
        using var scope = Services.CreateScope();
        var actual = scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.GetDbConnection().Database;

        if (!string.Equals(actual, expectedDatabase, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The app under test is connected to '{actual}' instead of the temporary test database '{expectedDatabase}'.");
        }
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, configuration) =>
            configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Default"] = connectionString,
            }));

        builder.ConfigureLogging(logging =>
        {
            logging.AddProvider(Logs);

            if (logSql)
            {
                logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Information);
            }
        });
    }
}
