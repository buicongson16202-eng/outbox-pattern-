using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using outbox_pattern.Inventory;
using outbox_pattern.Messaging;

namespace outbox_pattern.Tests.Infrastructure;

/// <summary>
/// Ráp relay → broker → consumer bằng chính các class production (OutboxProcessor, InMemoryBroker, InventoryConsumer),
/// trỏ vào database tạm, và cho test bật/tắt từng phần. Không có seam nào trong production: các BackgroundService
/// được điều khiển bằng StartAsync/StopAsync công khai của chúng, còn broker/publisher được tiêm qua constructor sẵn có.
/// </summary>
public sealed class OutboxHarness : IAsyncDisposable
{
    private readonly ServiceProvider _services;
    private readonly OutboxProcessor _relay;
    private readonly InventoryConsumer _consumer;
    private bool _started;

    public OutboxHarness(LocalDbFixture db, TimeSpan? maxBackoff = null)
    {
        _services = db.CreateServices();

        Broker = new InMemoryBroker(NullLogger<InMemoryBroker>.Instance);
        Publisher = new RecordingPublisher(Broker);
        RelayLog = new ListLogger<OutboxProcessor>();
        ConsumerLog = new ListLogger<InventoryConsumer>();

        var scopes = _services.GetRequiredService<IServiceScopeFactory>();
        _relay = new OutboxProcessor(scopes, Publisher, TestOptions.Outbox(maxBackoff), RelayLog);
        _consumer = new InventoryConsumer(Broker, scopes, ConsumerLog);
    }

    public InMemoryBroker Broker { get; }

    /// <summary>Mọi lần relay gọi broker đều đi qua đây nên đếm được số lần publish của từng message.</summary>
    public RecordingPublisher Publisher { get; }

    public ListLogger<OutboxProcessor> RelayLog { get; }

    public ListLogger<InventoryConsumer> ConsumerLog { get; }

    public int DuplicatesSkipped => ConsumerLog.Count("Duplicate message");

    public int MessagesApplied => ConsumerLog.Count(": reserved ");

    public string Diagnostics() =>
        $"-- relay log --{Environment.NewLine}{RelayLog.Dump()}{Environment.NewLine}-- consumer log --{Environment.NewLine}{ConsumerLog.Dump()}";

    public async Task StartAsync()
    {
        _started = true;
        await _relay.StartAsync(CancellationToken.None);
        await _consumer.StartAsync(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await _relay.StopAsync(timeout.Token);
            await _consumer.StopAsync(timeout.Token);
        }

        _relay.Dispose();
        _consumer.Dispose();
        await _services.DisposeAsync();
    }
}
