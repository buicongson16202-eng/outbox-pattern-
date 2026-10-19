using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.Contracts;
using outbox_pattern.Domain;
using outbox_pattern.Messaging;
using outbox_pattern.Persistence;

namespace outbox_pattern.Controllers
{
    /// <summary>Chỉ cho phép gọi khi ASPNETCORE_ENVIRONMENT=Development; môi trường khác trả về 404.</summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class DevelopmentOnlyAttribute : ActionFilterAttribute
    {
        public override void OnActionExecuting(ActionExecutingContext context)
        {
            var environment = context.HttpContext.RequestServices.GetRequiredService<IHostEnvironment>();

            if (!environment.IsDevelopment())
            {
                context.Result = new NotFoundResult();
            }
        }
    }

    /// <summary>Các endpoint phục vụ quan sát và tái hiện sự cố khi học/demo outbox pattern.</summary>
    [ApiController]
    [DevelopmentOnly]
    [Route("[controller]")]
    public class DemoController(AppDbContext db, InMemoryBroker broker) : ControllerBase
    {
        /// <summary>Bật/tắt/làm hỏng broker giả lập: PUT /demo/broker/up | down | loseack.</summary>
        [HttpPut("broker/{mode}")]
        public IActionResult SetBrokerMode(BrokerMode mode)
        {
            broker.Mode = mode;

            return Ok(new { broker = broker.Mode.ToString() });
        }

        /// <summary>Ảnh chụp toàn bộ trạng thái: outbox, inbox, tồn kho và các message đang kẹt.</summary>
        [HttpGet("state")]
        public async Task<IActionResult> GetState(CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;
            var outbox = db.OutboxMessages.AsNoTracking();

            var pendingMessages = await outbox
                .Where(m => m.ProcessedOnUtc == null)
                .OrderBy(m => m.OccurredOnUtc)
                .Take(10)
                .Select(m => new { m.Id, m.Type, m.Attempts, m.NextAttemptOnUtc, m.LastError })
                .ToListAsync(cancellationToken);

            var oldestPending = await outbox
                .Where(m => m.ProcessedOnUtc == null)
                .MinAsync(m => (DateTime?)m.OccurredOnUtc, cancellationToken);

            return Ok(new
            {
                broker = broker.Mode.ToString(),
                orders = await db.Orders.CountAsync(cancellationToken),
                outbox = new
                {
                    pending = await outbox.CountAsync(m => m.ProcessedOnUtc == null, cancellationToken),
                    processed = await outbox.CountAsync(m => m.ProcessedOnUtc != null, cancellationToken),
                    oldestPendingAgeSeconds = oldestPending is null ? (double?)null : Math.Round((now - oldestPending.Value).TotalSeconds, 1),
                    pendingMessages,
                },
                inbox = await db.InboxMessages.AsNoTracking().CountAsync(cancellationToken),
                stock = await db.StockItems.AsNoTracking().OrderBy(s => s.Sku).ToListAsync(cancellationToken),
            });
        }

        /// <summary>
        /// ANTI-PATTERN (dual write) — tồn tại chỉ để đối chiếu với POST /orders.
        /// Ghi database xong rồi mới gọi broker: hai hệ thống, hai lần ghi, không có transaction chung.
        /// </summary>
        [HttpPost("orders-naive")]
        public async Task<ActionResult<OrderResponse>> CreateOrderNaive(CreateOrderRequest request, CancellationToken cancellationToken)
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
            await db.SaveChangesAsync(cancellationToken); // (1) Đã COMMIT: order nằm trong database.

            // (2) Nếu broker lỗi, hoặc process chết ngay tại đây, order đã được lưu nhưng event MẤT VĨNH VIỄN —
            //     không còn dấu vết nào để thử lại. Đảo thứ tự (publish trước, lưu sau) thì lỗi ngược lại:
            //     event "ma" cho một order chưa từng tồn tại.
            var orderCreated = new OrderCreatedEvent(order.Id, order.Sku, order.Quantity, order.CustomerEmail, now);
            await broker.PublishAsync(
                new BrokerMessage(Guid.NewGuid(), OrderCreatedEvent.TypeName, order.Id.ToString(), JsonSerializer.Serialize(orderCreated), now),
                cancellationToken);

            return Ok(OrderResponse.From(order));
        }
    }
}
