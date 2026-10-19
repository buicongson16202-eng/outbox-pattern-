using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using outbox_pattern.Persistence;

namespace outbox_pattern.Tests.Infrastructure;

/// <summary>
/// Một database SQL Server LocalDB dùng một lần cho cả lần chạy test: tên duy nhất, tạo schema bằng đúng
/// <see cref="AppDbContext"/> của production, và được DROP khi kết thúc (kể cả khi tiến trình bị Ctrl+C).
/// Các test dùng chung database nên chạy tuần tự (cùng một collection) và gọi <see cref="ResetAsync"/> trước mỗi test.
/// </summary>
public sealed class LocalDbFixture : IAsyncLifetime
{
    public const string Server = @"(localdb)\MSSQLLocalDB";
    public const int InitialStock = 1000;

    private readonly string _masterConnectionString;
    private readonly object _dropLock = new();
    private OutboxApiFactory? _api;
    private bool _created;
    private bool _dropped;

    public LocalDbFixture()
    {
        DatabaseName = $"OutboxPatternTests_{Guid.NewGuid():N}";
        _masterConnectionString = BuildConnectionString("master");
        ConnectionString = BuildConnectionString(DatabaseName);
    }

    public string DatabaseName { get; }

    public string ConnectionString { get; }

    /// <summary>API thật (Program.cs) chạy trong bộ nhớ, trỏ vào database tạm. Các hosted service bị gỡ, xem <see cref="OutboxApiFactory"/>.</summary>
    public OutboxApiFactory Api => _api ??= CreateApi();

    public async Task InitializeAsync()
    {
        // Phòng khi tiến trình bị dừng đột ngột (Ctrl+C): vẫn cố DROP để không để lại database trong LocalDB.
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

        await using (var master = new SqlConnection(_masterConnectionString))
        {
            await master.OpenAsync();
            _created = true;
            await using var create = new SqlCommand($"CREATE DATABASE [{DatabaseName}]", master);
            await create.ExecuteNonQueryAsync();
        }

        // Database đã tồn tại nhưng rỗng → EnsureCreated chỉ tạo bảng (và dữ liệu seed SKU-001..003).
        await using var db = CreateDbContext();
        await db.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync()
    {
        AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;
        _api?.Dispose();
        DropDatabase();
        return Task.CompletedTask;
    }

    public AppDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(ConnectionString).Options);

    /// <summary>Container DI tối thiểu (chỉ DbContext) để chạy OutboxProcessor / InventoryConsumer ngoài web host.</summary>
    public ServiceProvider CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(options => options.UseSqlServer(ConnectionString));
        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>Đưa database về trạng thái ban đầu: không có order/outbox/inbox, mỗi SKU còn <see cref="InitialStock"/>.</summary>
    public Task ResetAsync() => ExecuteAsync($"""
        IF OBJECT_ID(N'dbo.TR_Test_FailOutboxInsert', N'TR') IS NOT NULL DROP TRIGGER dbo.TR_Test_FailOutboxInsert;
        DELETE FROM InboxMessages;
        DELETE FROM OutboxMessages;
        DELETE FROM Orders;
        DELETE FROM StockItems;
        INSERT INTO StockItems (Sku, Available) VALUES (N'SKU-001', {InitialStock}), (N'SKU-002', {InitialStock}), (N'SKU-003', {InitialStock});
        """);

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync())!, typeof(T));
    }

    private OutboxApiFactory CreateApi()
    {
        var factory = new OutboxApiFactory(ConnectionString);

        // Chốt chặn an toàn: tuyệt đối không để test ghi nhầm vào database demo (OutboxPatternDemo trong appsettings.json).
        using var scope = factory.Services.CreateScope();
        var actual = scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.GetDbConnection().Database;
        if (!string.Equals(actual, DatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            factory.Dispose();
            throw new InvalidOperationException(
                $"The API under test is connected to '{actual}' instead of the temporary test database '{DatabaseName}'.");
        }

        return factory;
    }

    private string BuildConnectionString(string database) => new SqlConnectionStringBuilder
    {
        DataSource = Server,
        InitialCatalog = database,
        IntegratedSecurity = true,
        TrustServerCertificate = true,
        ConnectTimeout = 60, // lần kết nối đầu tiên có thể phải khởi động instance LocalDB
        ApplicationName = "outbox_pattern.Tests",
    }.ConnectionString;

    private void OnProcessExit(object? sender, EventArgs e) => DropDatabase();

    private void DropDatabase()
    {
        lock (_dropLock)
        {
            if (!_created || _dropped)
            {
                return;
            }

            // Trả hết connection còn nằm trong pool, rồi ép mọi session còn lại thoát trước khi DROP.
            SqlConnection.ClearAllPools();

            using var connection = new SqlConnection(_masterConnectionString);
            connection.Open();

            using (var drop = new SqlCommand($"""
                IF DB_ID(N'{DatabaseName}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{DatabaseName}];
                END
                """, connection))
            {
                drop.ExecuteNonQuery();
            }

            using var check = new SqlCommand($"SELECT DB_ID(N'{DatabaseName}')", connection);
            if (check.ExecuteScalar() is not DBNull)
            {
                throw new InvalidOperationException($"Test database '{DatabaseName}' still exists after DROP DATABASE.");
            }

            _dropped = true;
        }
    }
}

[CollectionDefinition(Name)]
public sealed class LocalDbCollection : ICollectionFixture<LocalDbFixture>
{
    public const string Name = "LocalDb";
}
