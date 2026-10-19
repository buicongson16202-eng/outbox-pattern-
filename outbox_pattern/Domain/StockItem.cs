namespace outbox_pattern.Domain
{
    /// <summary>
    /// Tồn kho do "Inventory service" sở hữu. Chỉ consumer được phép thay đổi khi nhận OrderCreated,
    /// nên nếu một message bị xử lý hai lần thì tồn kho sẽ bị trừ sai — đó là lý do cần inbox.
    /// </summary>
    public class StockItem
    {
        public string Sku { get; set; } = string.Empty;

        public int Available { get; set; }
    }
}
