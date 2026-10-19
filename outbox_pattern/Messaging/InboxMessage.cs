namespace outbox_pattern.Messaging
{
    /// <summary>
    /// Sổ ghi "consumer này đã xử lý message này rồi". Khoá chính (MessageId, Consumer) nên mỗi consumer
    /// khử trùng lặp độc lập: cùng một message có thể được nhiều consumer khác nhau xử lý, mỗi bên đúng một lần.
    /// </summary>
    public class InboxMessage
    {
        public Guid MessageId { get; set; }

        public string Consumer { get; set; } = string.Empty;

        public DateTime ProcessedOnUtc { get; set; }
    }
}
