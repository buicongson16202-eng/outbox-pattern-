namespace outbox_pattern.Contracts
{
    /// <summary>
    /// Integration event: hợp đồng giữa Orders (producer) và Inventory (consumer).
    /// TypeName là tên logic ổn định, không gắn với tên class CLR — đổi tên/namespace class không làm vỡ
    /// các message đang nằm trong outbox hay broker, và hậu tố ".v1" cho phép tiến hoá schema về sau.
    /// </summary>
    public sealed record OrderCreatedEvent(
        Guid OrderId,
        string Sku,
        int Quantity,
        string CustomerEmail,
        DateTime OccurredOnUtc)
    {
        public const string TypeName = "orders.order-created.v1";
    }
}
