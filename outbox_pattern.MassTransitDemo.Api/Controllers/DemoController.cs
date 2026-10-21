using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using outbox_pattern.MassTransitDemo.Infrastructure.Demo;

namespace outbox_pattern.MassTransitDemo.Api.Controllers
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

    /// <summary>
    /// Các endpoint phục vụ quan sát và tái hiện sự cố khi học/demo MassTransit outbox. Ngoại lệ có chủ đích của Clean
    /// Architecture: chúng gọi thẳng công cụ ở Infrastructure (DemoTools, các công tắc) chứ không qua use case.
    /// </summary>
    [ApiController]
    [DevelopmentOnly]
    [Route("[controller]")]
    public class DemoController(DemoTools tools, ConsumerFaults faults, TransportSwitch transport) : ControllerBase
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

        /// <summary>Ảnh chụp trạng thái: order, ba bảng của MassTransit (OutboxMessage, OutboxState, InboxState) và tồn kho.</summary>
        [HttpGet("state")]
        public async Task<IActionResult> GetState(CancellationToken cancellationToken) =>
            Ok(await tools.GetStateAsync(cancellationToken));

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
            copies = Math.Clamp(copies, 1, 20);

            var (orderId, messageId) = await tools.CreateOrderWithDuplicateMessagesAsync(
                request.Sku, request.Quantity, request.CustomerEmail, copies, cancellationToken);

            return Ok(new { orderId, messageId, copies });
        }
    }
}
