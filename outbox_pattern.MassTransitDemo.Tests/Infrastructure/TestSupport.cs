using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace outbox_pattern.MassTransitDemo.Tests.Infrastructure;

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

public sealed record LogEntry(string Category, LogLevel Level, string Message, Exception? Exception)
{
    public DateTime At { get; } = DateTime.UtcNow;
}

/// <summary>Provider ghi lại mọi dòng log của ứng dụng trong bộ nhớ để test đếm/kiểm tra (vd. số lần consumer ghi "reserved").</summary>
public sealed class LogSink : ILoggerProvider
{
    private readonly ConcurrentQueue<LogEntry> _entries = new();

    public IReadOnlyList<LogEntry> Entries => _entries.ToArray();

    public void Clear() => _entries.Clear();

    /// <summary>Có dòng log nào kèm exception (hoặc inner exception) mang thông điệp chứa <paramref name="text"/> không.</summary>
    public bool HasExceptionContaining(string text) =>
        _entries.Any(e =>
        {
            for (var ex = e.Exception; ex is not null; ex = ex.InnerException)
            {
                if (ex.Message.Contains(text, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        });

    public int Count(string text) =>
        _entries.Count(e => e.Message.Contains(text, StringComparison.Ordinal));

    public string Dump(int max = 80) => string.Join(
        Environment.NewLine,
        _entries.TakeLast(max).Select(e =>
            $"{e.At:HH:mm:ss.fff} [{e.Level}] {e.Category}: {e.Message}{(e.Exception is null ? string.Empty : " :: " + e.Exception.GetType().Name + ": " + e.Exception.Message)}"));

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, _entries);

    public void Dispose()
    {
    }

    private sealed class Logger(string category, ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            entries.Enqueue(new LogEntry(category, logLevel, formatter(state, exception), exception));
    }
}
