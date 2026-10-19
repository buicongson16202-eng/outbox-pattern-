namespace outbox_pattern.Domain
{
    public class Order
    {
        public Guid Id { get; set; }

        public string Sku { get; set; } = string.Empty;

        public int Quantity { get; set; }

        public string CustomerEmail { get; set; } = string.Empty;

        public DateTime CreatedOnUtc { get; set; }
    }
}
