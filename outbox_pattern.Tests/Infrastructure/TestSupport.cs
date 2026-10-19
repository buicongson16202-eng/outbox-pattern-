using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using outbox_pattern.Messaging;

namespace outbox_pattern.Tests.Infrastructure;

public static class Wait
{
    /// <summary>Thăm dò điều kiện mỗi 50 ms cho đến khi đúng; quá hạn thì fail kèm mô tả và thông tin chẩn đoán.</summary>
    public static async Task UntilAsync(
        Func<Task<bool>> condition,
        string description,
        TimeSpan? timeout = null,
        Func<string>? diagnostics = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(30);
        var stopwatch = Stopwatch.StartNew();

        while (!await condition())
        {
            if (stopwatch.Elapsed > limit)
            {
                Assert.Fail(
                    $"Timed out after {limit.TotalSeconds:0}s waiting until {description}." +
                    (diagnostics is null ? string.Empty : Environment.NewLine + diagnostics()));
            }

            await Task.Delay(50);
        }
    }
}

public static class TestOptions
{
    /// <summary>OutboxOptions cho test: quét thưa 50 ms thay vì 1 s; các giá trị còn lại giữ mặc định của production.</summary>
    public static IOptions<OutboxOptions> Outbox(TimeSpan? maxBackoff = null, int? batchSize = null)
    {
        var defaults = new OutboxOptions();

        return Options.Create(new OutboxOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(50),
            BatchSize = batchSize ?? defaults.BatchSize,
            PublishTimeout = defaults.PublishTimeout,
            MaxBackoff = maxBackoff ?? defaults.MaxBackoff,
        });
    }
}

public sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

/// <summary>ILogger ghi lại mọi dòng log trong bộ nhớ để test đếm/kiểm tra (vd. số lần "Duplicate message … skipped").</summary>
public sealed class ListLogger<T> : ILogger<T>
{
    private readonly ConcurrentQueue<LogEntry> _entries = new();

    public int Count(string text) =>
        _entries.Count(e => e.Message.Contains(text, StringComparison.Ordinal));

    public int ErrorCount => _entries.Count(e => e.Level >= LogLevel.Error);

    public string Dump() => string.Join(
        Environment.NewLine,
        _entries.Select(e => $"[{e.Level}] {e.Message}{(e.Exception is null ? string.Empty : " :: " + e.Exception.Message)}"));

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        _entries.Enqueue(new LogEntry(logLevel, formatter(state, exception), exception));
}

/// <summary>Bọc một publisher thật và đếm số lần relay gọi PublishAsync cho từng MessageId (kể cả các lần ném lỗi).</summary>
public sealed class RecordingPublisher(IMessagePublisher inner) : IMessagePublisher
{
    private readonly ConcurrentDictionary<Guid, int> _calls = new();

    public int CallsFor(Guid messageId) => _calls.GetValueOrDefault(messageId);

    public int TotalCalls => _calls.Values.Sum();

    public ValueTask PublishAsync(BrokerMessage message, CancellationToken cancellationToken)
    {
        _calls.AddOrUpdate(message.Id, 1, (_, count) => count + 1);
        return inner.PublishAsync(message, cancellationToken);
    }
}
