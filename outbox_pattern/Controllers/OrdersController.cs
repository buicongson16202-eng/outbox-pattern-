using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.Contracts;
using outbox_pattern.Domain;
using outbox_pattern.Messaging;
using outbox_pattern.Persistence;

namespace outbox_pattern.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class OrdersController(AppDbContext db) : ControllerBase
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

            var orderCreated = new OrderCreatedEvent(order.Id, order.Sku, order.Quantity, order.CustomerEmail, now);

            db.Orders.Add(order);
            db.OutboxMessages.Add(
                OutboxMessage.Create(OrderCreatedEvent.TypeName, aggregateId: order.Id.ToString(), orderCreated, now));

            // ĐÂY LÀ ĐIỂM MẤU CHỐT CỦA PATTERN. Order và OutboxMessage nằm trong CÙNG một SaveChanges nên EF gửi cả hai
            // INSERT trong một transaction duy nhất của database: hoặc cả hai được commit, hoặc cả hai bị rollback.
            // Không có cửa sổ nào để "order đã lưu nhưng event mất" hay "event đã gửi nhưng order bị rollback".
            // Controller KHÔNG gọi broker. Việc publish do OutboxProcessor đảm nhiệm, bất đồng bộ, sau khi commit.
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
