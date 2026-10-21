using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Contracts;
using outbox_pattern.MassTransitDemo.Domain;
using outbox_pattern.MassTransitDemo.Inventory;
using outbox_pattern.MassTransitDemo.Messaging;
using outbox_pattern.MassTransitDemo.Persistence;

namespace outbox_pattern.MassTransitDemo.Controllers
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

    /// <summary>Các endpoint phục vụ quan sát và tái hiện sự cố khi học/demo MassTransit outbox.</summary>
    [ApiController]
    [DevelopmentOnly]
    [Route("[controller]")]
    public class DemoController(
        AppDbContext db, IPublishEndpoint publishEndpoint, ConsumerFaults faults, TransportSwitch transport) : ControllerBase
    {
        /// <summary>Chế độ transport giả lập: PUT /demo/transport/up | down (sập, ném lỗi) | hang (treo, chờ vô hạn).</summary>
        [HttpPut("transport/{mode}")]
        public IActionResult SetTransportMode(string mode)
        {
            switch (mode.ToLowerInvariant())
            {
                case "up":
                    transport.SetUp();
                    break;
                case "down":
                    transport.SetDown(true);
                    break;
                case "hang":
                    transport.Hang();
                    break;
                default:
                    return BadRequest("mode must be 'up', 'down' or 'hang'");
            }

            return Ok(new { transport = transport.Mode });
        }

        /// <summary>
        /// Ảnh chụp trạng thái: order, ba bảng của MassTransit (OutboxMessage, OutboxState, InboxState) và tồn kho.
        /// OutboxMessage.messages = số dòng đang chờ giao (dòng đã giao được dịch vụ giao outbox xoá đi).
        /// </summary>
        [HttpGet("state")]
        public async Task<IActionResult> GetState(CancellationToken cancellationToken)
        {
            var outboxMessages = db.Set<OutboxMessage>().AsNoTracking();
            var inbox = db.Set<InboxState>().AsNoTracking();

            return Ok(new
            {
                transport = transport.Mode,
                orders = await db.Orders.CountAsync(cancellationToken),
                outbox = new
                {
                    waitingMessages = await outboxMessages.CountAsync(cancellationToken),
                    waiting = await outboxMessages
                        .OrderBy(m => m.SequenceNumber)
                        .Take(10)
                        .Select(m => new { m.SequenceNumber, m.MessageId, m.MessageType, m.EnqueueTime })
                        .ToListAsync(cancellationToken),
                    states = await db.Set<OutboxState>().AsNoTracking().CountAsync(cancellationToken),
                    undeliveredStates = await db.Set<OutboxState>().AsNoTracking().CountAsync(s => s.Delivered == null, cancellationToken),
                },
                inbox = new
                {
                    total = await inbox.CountAsync(cancellationToken),
                    consumed = await inbox.CountAsync(i => i.Consumed != null, cancellationToken),
                    receiveCount = await inbox.SumAsync(i => i.ReceiveCount, cancellationToken),
                },
                consumerFailuresArmed = faults.Remaining,
                stock = await db.StockItems.AsNoTracking().OrderBy(s => s.Sku).ToListAsync(cancellationToken),
            });
        }

        /// <summary>Bắt consumer ném lỗi N lần tiếp theo: PUT /demo/consumer/fail?times=2 (0 để tắt).</summary>
        [HttpPut("consumer/fail")]
        public IActionResult FailConsumer([FromQuery] int times)
        {
            faults.Arm(times);

            return Ok(new { consumerFailuresArmed = faults.Remaining });
        }

        /// <summary>
        /// Tái hiện bản trùng: tạo một order nhưng đưa CÙNG MỘT message (cùng MessageId) vào outbox nhiều lần,
        /// như khi broker giao lại. Consumer vẫn chỉ trừ kho một lần nhờ inbox.
        /// </summary>
        [HttpPost("orders-duplicate")]
        public async Task<IActionResult> CreateOrderWithDuplicateMessages(
            CreateOrderRequest request, [FromQuery] int copies = 3, CancellationToken cancellationToken = default)
        {
            var now = DateTime.UtcNow;
            var messageId = Guid.NewGuid();

            var order = new Order
            {
                Id = Guid.NewGuid(),
                Sku = request.Sku,
                Quantity = request.Quantity,
                CustomerEmail = request.CustomerEmail,
                CreatedOnUtc = now,
            };

            db.Orders.Add(order);

            for (var i = 0; i < Math.Clamp(copies, 1, 20); i++)
            {
                await publishEndpoint.Publish(
                    new OrderCreated(order.Id, order.Sku, order.Quantity, order.CustomerEmail, now),
                    context => context.MessageId = messageId,
                    cancellationToken);
            }

            await db.SaveChangesAsync(cancellationToken);

            return Ok(new { orderId = order.Id, messageId, copies });
        }
    }
}
