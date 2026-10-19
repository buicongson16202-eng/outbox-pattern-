namespace outbox_pattern.Messaging
{
    public class OutboxOptions
    {
        public const string SectionName = "Outbox";

        /// <summary>Chu kỳ relay quét bảng outbox khi không còn việc để làm.</summary>
        public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Số message tối đa mỗi lần claim. Nhỏ → giữ lock ngắn, ít tranh chấp giữa các instance;
        /// lớn → throughput cao hơn nhưng một lô lỗi giữ lock lâu hơn.
        /// </summary>
        public int BatchSize { get; set; } = 20;

        /// <summary>
        /// Thời gian tối đa cho MỘT lần publish. Bắt buộc phải có: relay đang giữ transaction + row-lock,
        /// một lời gọi broker treo vô hạn sẽ giữ lock vô hạn.
        /// </summary>
        public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>Backoff gấp đôi sau mỗi lần lỗi (2s, 4s, 8s…) nhưng không vượt quá giá trị này.</summary>
        public TimeSpan MaxBackoff { get; set; } = TimeSpan.FromMinutes(1);

        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>Giữ outbox đã publish bao lâu (phục vụ debug/audit) trước khi xoá.</summary>
        public TimeSpan OutboxRetention { get; set; } = TimeSpan.FromDays(7);

        /// <summary>
        /// Phải LỚN HƠN khoảng thời gian dài nhất mà một message còn có thể bị giao lại (broker sập + backoff + redelivery).
        /// Xoá inbox sớm hơn thì một bản trùng đến muộn sẽ lại được xử lý như message mới.
        /// </summary>
        public TimeSpan InboxRetention { get; set; } = TimeSpan.FromDays(14);
    }
}
