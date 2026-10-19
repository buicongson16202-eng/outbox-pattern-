using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using outbox_pattern.Controllers;
using outbox_pattern.Messaging;

namespace outbox_pattern.Tests.Infrastructure;

/// <summary>
/// Chạy đúng Program.cs của project production trong bộ nhớ. Hai điều chỉnh, đều làm từ phía test:
/// (1) trỏ connection string vào database tạm; (2) gỡ các hosted service của project (relay, consumer, cleanup)
/// để các test tự quyết định lúc nào relay/consumer chạy — nếu không, chúng sẽ giao message trước khi test kịp quan sát
/// dòng outbox đang ở trạng thái "pending".
/// <see cref="OrdersController"/> chỉ dùng để chỉ ra assembly của ứng dụng: <c>Program</c> là internal.
/// </summary>
public sealed class OutboxApiFactory(string connectionString) : WebApplicationFactory<OrdersController>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, configuration) =>
            configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Default"] = connectionString,
            }));

        builder.ConfigureServices(services =>
        {
            var appAssembly = typeof(OutboxProcessor).Assembly;
            var hostedServices = services
                .Where(d => d.ServiceType == typeof(IHostedService) && d.ImplementationType?.Assembly == appAssembly)
                .ToList();

            foreach (var descriptor in hostedServices)
            {
                services.Remove(descriptor);
            }
        });
    }
}
