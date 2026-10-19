namespace outbox_pattern.Messaging
{
    /// <summary>Thứ relay gửi đi và consumer nhận về — tương đương một message nằm trên broker.</summary>
    public sealed record BrokerMessage(
        Guid Id,
        string Type,
        string Key,
        string Payload,
        DateTime OccurredOnUtc);
}
