using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using outbox_pattern.MassTransitDemo.Application.Inventory;
using outbox_pattern.MassTransitDemo.Application.Orders;

namespace outbox_pattern.MassTransitDemo.Application
{
    public static class DependencyInjection
    {
        /// <summary>Đăng ký các use case. Các cổng (IOrderRepository, IOrderEventPublisher…) do AddInfrastructure đăng ký.</summary>
        public static IServiceCollection AddApplication(this IServiceCollection services)
        {
            services.TryAddSingleton(TimeProvider.System);

            services.AddScoped<CreateOrderHandler>();
            services.AddScoped<GetOrderHandler>();
            services.AddScoped<ReserveStockHandler>();

            return services;
        }
    }
}
