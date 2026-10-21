using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using outbox_pattern.MassTransitDemo.Application.Orders;
using outbox_pattern.MassTransitDemo.Domain.Common;
using outbox_pattern.MassTransitDemo.Domain.Orders;

namespace outbox_pattern.MassTransitDemo.Api.Controllers
{
    /// <summary>
    /// Controller mỏng: kiểm tra định dạng request, gọi use case, dịch kết quả ra HTTP. Không biết EF Core hay MassTransit —
    /// việc ghi Order + message trong một transaction nằm ở CreateOrderHandler và các cổng mà Infrastructure cài đặt.
    /// </summary>
    [ApiController]
    [Route("[controller]")]
    public class OrdersController(CreateOrderHandler createOrder, GetOrderHandler getOrder) : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult<OrderDto>> Create(CreateOrderRequest request, CancellationToken cancellationToken)
        {
            try
            {
                var order = await createOrder.HandleAsync(
                    new CreateOrderCommand(request.Sku, request.Quantity, request.CustomerEmail),
                    cancellationToken);

                return CreatedAtAction(nameof(Get), new { id = order.Id }, order);
            }
            catch (DomainException ex)
            {
                return ValidationProblem(detail: ex.Message);
            }
        }

        [HttpGet("{id:guid}")]
        public async Task<ActionResult<OrderDto>> Get(Guid id, CancellationToken cancellationToken)
        {
            var order = await getOrder.HandleAsync(id, cancellationToken);

            return order is null ? NotFound() : order;
        }
    }

    public class CreateOrderRequest
    {
        [Required, StringLength(Order.MaxSkuLength)]
        public string Sku { get; set; } = string.Empty;

        [Range(1, Order.MaxQuantity)]
        public int Quantity { get; set; }

        [Required, EmailAddress, StringLength(Order.MaxEmailLength)]
        public string CustomerEmail { get; set; } = string.Empty;
    }
}
