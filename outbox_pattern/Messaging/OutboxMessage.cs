using System.Text.Json;

namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Một dòng trong bảng outbox: "lời hứa" sẽ publish một event. Dòng này được ghi CÙNG transaction với
    /// dữ liệu nghiệp vụ nên hai thứ hoặc cùng tồn tại, hoặc cùng không tồn tại.
    /// </summary>
    public class OutboxMessage
    {
        // Id là MessageId xuyên suốt hệ thống: relay gửi nó đi, consumer dùng nó để khử trùng lặp (inbox).
        public Guid Id { get; set; }

        // Tên logic của event, vd. "orders.order-created.v1" — consumer dựa vào đây để chọn handler.
        public string Type { get; set; } = string.Empty;

        // Khoá thứ tự/partition (vd. OrderId): các event cùng khoá cần được xử lý đúng thứ tự.
        public string AggregateId { get; set; } = string.Empty;

        // JSON. Relay chỉ chuyển tiếp nguyên văn, không cần biết schema của event.
        public string Payload { get; set; } = string.Empty;

        public DateTime OccurredOnUtc { get; set; }

        // null = chưa publish xong.
        public DateTime? ProcessedOnUtc { get; set; }

        public int Attempts { get; set; }

        // Sớm nhất khi nào relay được phép thử publish lại (exponential backoff).
        public DateTime NextAttemptOnUtc { get; set; }

        public string? LastError { get; set; }

        public static OutboxMessage Create<T>(string type, string aggregateId, T payload, DateTime utcNow) => new()
        {
            Id = Guid.NewGuid(),
            Type = type,
            AggregateId = aggregateId,
            Payload = JsonSerializer.Serialize(payload),
            OccurredOnUtc = utcNow,
            NextAttemptOnUtc = utcNow,
        };
    }
}
