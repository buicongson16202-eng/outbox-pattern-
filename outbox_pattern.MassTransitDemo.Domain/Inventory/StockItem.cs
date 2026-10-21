namespace outbox_pattern.MassTransitDemo.Domain.Inventory
{
    /// <summary>
    /// Tồn kho do "Inventory service" sở hữu. Chỉ consumer được phép thay đổi khi nhận OrderCreated.
    ///
    /// Quy tắc "không giữ chỗ quá số còn lại" KHÔNG viết thành method đọc-sửa-ghi trên entity này: nhiều consumer có thể trừ
    /// cùng một SKU đồng thời, và kiểu đọc về rồi ghi lại sẽ làm mất cập nhật. Quy tắc được thực thi bằng một câu UPDATE
    /// có điều kiện (Available >= qty) trong IStockRepository.TryReserveAsync — xem OUTBOX_PATTERN.md mục 9.3, case C3.
    /// </summary>
    public sealed class StockItem
    {
        // Cho EF Core dựng lại entity từ database.
        private StockItem()
        {
        }

        public StockItem(string sku, int available)
        {
            Sku = sku;
            Available = available;
        }

        public string Sku { get; private set; } = string.Empty;

        public int Available { get; private set; }
    }
}
