using System.ComponentModel.DataAnnotations;
using MassTransit;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Contracts;
using outbox_pattern.MassTransitDemo.Domain;
using outbox_pattern.MassTransitDemo.Persistence;

namespace outbox_pattern.MassTransitDemo.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OrdersController(AppDbContext db, IPublishEndpoint publishEndpoint) : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult<OrderResponse>> Create(CreateOrderRequest request, CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            var order = new Order
            {
                Id = Guid.NewGuid(),
                Sku = request.Sku,
                Quantity = request.Quantity,
                CustomerEmail = request.CustomerEmail,
                CreatedOnUtc = now,
            };

            db.Orders.Add(order);

            // Nhờ UseBusOutbox() (Program.cs), IPublishEndpoint ở đây KHÔNG gửi lên broker. Nó thêm một dòng OutboxMessage
            // vào chính DbContext này; dòng đó chỉ được ghi xuống database khi gọi SaveChangesAsync bên dưới.
            await publishEndpoint.Publish(
                new OrderCreated(order.Id, order.Sku, order.Quantity, order.CustomerEmail, now),
                cancellationToken);

            // ĐÂY LÀ ĐIỂM MẤU CHỐT CỦA PATTERN (giống project outbox_pattern): Order và OutboxMessage nằm trong CÙNG một
            // SaveChanges nên cùng một transaction của database: hoặc cả hai được commit, hoặc cả hai bị rollback.
            // Việc đẩy message sang broker do BusOutboxDeliveryService của MassTransit đảm nhiệm, bất đồng bộ, sau khi commit.
            await db.SaveChangesAsync(cancellationToken);

            return CreatedAtAction(nameof(Get), new { id = order.Id }, OrderResponse.From(order));
        }

        [HttpGet("{id:guid}")]
        public async Task<ActionResult<OrderResponse>> Get(Guid id, CancellationToken cancellationToken)
        {
            var order = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id, cancellationToken);

            return order is null ? NotFound() : OrderResponse.From(order);
        }
    }

    public class CreateOrderRequest
    {
        [Required, StringLength(50)]
        public string Sku { get; set; } = string.Empty;

        [Range(1, 1000)]
        public int Quantity { get; set; }

        [Required, EmailAddress, StringLength(320)]
        public string CustomerEmail { get; set; } = string.Empty;
    }

    public record OrderResponse(Guid Id, string Sku, int Quantity, string CustomerEmail, DateTime CreatedOnUtc)
    {
        public static OrderResponse From(Order order) =>
            new(order.Id, order.Sku, order.Quantity, order.CustomerEmail, order.CreatedOnUtc);
    }
}
