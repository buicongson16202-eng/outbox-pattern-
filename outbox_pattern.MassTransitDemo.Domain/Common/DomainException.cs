namespace outbox_pattern.MassTransitDemo.Domain.Common
{
    /// <summary>Vi phạm quy tắc nghiệp vụ (bất biến của entity). API trả về 400 cho lỗi này.</summary>
    public class DomainException(string message) : Exception(message);
}
