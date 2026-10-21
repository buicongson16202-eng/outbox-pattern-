namespace outbox_pattern.MassTransitDemo.Domain
{
    /// <summary>
    /// Tồn kho do "Inventory service" sở hữu. Chỉ consumer được phép thay đổi khi nhận OrderCreated.
    /// Phép trừ không idempotent, nên nếu một message bị xử lý hai lần thì kho sai — inbox của MassTransit chặn việc đó.
    /// </summary>
    public class StockItem
    {
        public string Sku { get; set; } = string.Empty;

        public int Available { get; set; }
    }
}
