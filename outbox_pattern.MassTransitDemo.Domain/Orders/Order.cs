using outbox_pattern.MassTransitDemo.Domain.Common;

namespace outbox_pattern.MassTransitDemo.Domain.Orders
{
    /// <summary>
    /// Đơn hàng. Chỉ tạo được qua <see cref="Create"/> nên một Order luôn hợp lệ; các property chỉ đọc từ bên ngoài.
    /// </summary>
    public sealed class Order
    {
        public const int MaxSkuLength = 50;
        public const int MaxEmailLength = 320;
        public const int MaxQuantity = 1000;

        // Cho EF Core dựng lại entity từ database.
        private Order()
        {
        }

        public Guid Id { get; private set; }

        public string Sku { get; private set; } = string.Empty;

        public int Quantity { get; private set; }

        public string CustomerEmail { get; private set; } = string.Empty;

        public DateTime CreatedOnUtc { get; private set; }

        public static Order Create(string sku, int quantity, string customerEmail, DateTime createdOnUtc)
        {
            if (string.IsNullOrWhiteSpace(sku) || sku.Length > MaxSkuLength)
            {
                throw new DomainException($"SKU is required and must be at most {MaxSkuLength} characters.");
            }

            if (quantity is < 1 or > MaxQuantity)
            {
                throw new DomainException($"Quantity must be between 1 and {MaxQuantity}.");
            }

            if (string.IsNullOrWhiteSpace(customerEmail) || customerEmail.Length > MaxEmailLength)
            {
                throw new DomainException($"Customer email is required and must be at most {MaxEmailLength} characters.");
            }

            return new Order
            {
                Id = Guid.NewGuid(),
                Sku = sku,
                Quantity = quantity,
                CustomerEmail = customerEmail,
                CreatedOnUtc = createdOnUtc,
            };
        }
    }
}
