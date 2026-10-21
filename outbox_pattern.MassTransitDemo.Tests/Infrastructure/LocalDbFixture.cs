using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using outbox_pattern.MassTransitDemo.Persistence;

namespace outbox_pattern.MassTransitDemo.Tests.Infrastructure;

/// <summary>
/// Một database SQL Server LocalDB dùng một lần cho cả lần chạy test: tên duy nhất, tạo schema bằng đúng
/// <see cref="AppDbContext"/> của project (gồm cả ba bảng của MassTransit), và được DROP khi kết thúc (kể cả khi bị Ctrl+C).
/// Các test dùng chung database nên chạy tuần tự (cùng một collection) và gọi <see cref="ResetAsync"/> trước mỗi test.
/// </summary>
public sealed class LocalDbFixture : IAsyncLifetime
{
    /// <summary>Instance mặc định của LocalDB. Đặt biến môi trường OUTBOX_TESTS_SQL_SERVER để dùng instance/server khác.</summary>
    public static readonly string Server =
        Environment.GetEnvironmentVariable("OUTBOX_TESTS_SQL_SERVER") is { Length: > 0 } custom ? custom : @"(localdb)\MSSQLLocalDB";

    public const int InitialStock = 1000;

    private readonly string _masterConnectionString;
    private readonly object _dropLock = new();
    private bool _created;
    private bool _dropped;

    public LocalDbFixture()
    {
        DatabaseName = $"MassTransitDemoTests_{Guid.NewGuid():N}";
        _masterConnectionString = BuildConnectionString("master");
        ConnectionString = BuildConnectionString(DatabaseName);
    }

    public string DatabaseName { get; }

    public string ConnectionString { get; }

    /// <summary>
    /// Khởi động một instance của ứng dụng (Program.cs thật, gồm bus MassTransit và transport in-memory) trong bộ nhớ, trỏ vào
    /// database tạm. Gọi nhiều lần để có nhiều instance cùng dùng một database (mô phỏng scale-out). Người gọi tự Dispose.
    /// </summary>
    public DemoApiFactory CreateApi(bool logSql = false)
    {
        var factory = new DemoApiFactory(ConnectionString, logSql);
        factory.AssertConnectedTo(DatabaseName); // khởi động host và chốt chặn: không ghi nhầm vào database demo
        return factory;
    }

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
        DropDatabase();
        return Task.CompletedTask;
    }

    public AppDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(ConnectionString).Options);

    /// <summary>Đưa database về trạng thái ban đầu: không có order/outbox/inbox, mỗi SKU còn <see cref="InitialStock"/>.</summary>
    public Task ResetAsync() => ExecuteAsync($"""
        IF OBJECT_ID(N'dbo.CK_Test_FailOutboxInsert', N'C') IS NOT NULL ALTER TABLE dbo.OutboxMessage DROP CONSTRAINT CK_Test_FailOutboxInsert;
        DELETE FROM InboxState;
        DELETE FROM OutboxMessage;
        DELETE FROM OutboxState;
        DELETE FROM Orders;
        DELETE FROM StockItems;
        INSERT INTO StockItems (Sku, Available) VALUES (N'SKU-001', {InitialStock}), (N'SKU-002', {InitialStock}), (N'SKU-003', {InitialStock});
        """);

    public async Task ExecuteAsync(string sql)
    {
        // Các dịch vụ nền của ứng dụng (giao outbox, dọn inbox, consumer) vẫn chạy song song với test và có thể kẹt khoá với
        // câu lệnh của test; SQL Server chọn một bên làm "nạn nhân" deadlock (1205). Thử lại là đúng cách xử lý.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var connection = new SqlConnection(ConnectionString);
                await connection.OpenAsync();
                await using var command = new SqlCommand(sql, connection);
                await command.ExecuteNonQueryAsync();
                return;
            }
            catch (SqlException ex) when (ex.Number == 1205 && attempt < 5)
            {
                await Task.Delay(50 * attempt);
            }
        }
    }

    public async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync())!, typeof(T));
    }

    private string BuildConnectionString(string database) => new SqlConnectionStringBuilder
    {
        DataSource = Server,
        InitialCatalog = database,
        IntegratedSecurity = true,
        TrustServerCertificate = true,
        ConnectTimeout = 60, // lần kết nối đầu tiên có thể phải khởi động instance LocalDB
        ApplicationName = "outbox_pattern.MassTransitDemo.Tests",
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
