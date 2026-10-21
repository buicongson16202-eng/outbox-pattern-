using System.Reflection;
using outbox_pattern.MassTransitDemo.Api.Controllers;
using outbox_pattern.MassTransitDemo.Application.Orders;
using outbox_pattern.MassTransitDemo.Domain.Orders;
using outbox_pattern.MassTransitDemo.Infrastructure.Persistence;

namespace outbox_pattern.MassTransitDemo.Tests.Unit;

/// <summary>
/// Giữ "quy tắc phụ thuộc" của Clean Architecture: mũi tên phụ thuộc chỉ hướng VÀO trong
/// (Api → Infrastructure → Application → Domain). Test đọc danh sách assembly mà mỗi tầng thực sự dùng trong mã đã biên dịch.
/// </summary>
public class ArchitectureTests
{
    private static readonly Assembly Domain = typeof(Order).Assembly;
    private static readonly Assembly Application = typeof(CreateOrderHandler).Assembly;
    private static readonly Assembly Infrastructure = typeof(AppDbContext).Assembly;
    private static readonly Assembly Api = typeof(OrdersController).Assembly;

    [Fact]
    public void Domain_DependsOnNothingElseInTheSolution_NorOnAnyFramework()
    {
        AssertDoesNotReference(Domain, Application, Infrastructure, Api);
        AssertDoesNotReferenceFrameworks(Domain, "MassTransit", "Microsoft.EntityFrameworkCore", "Microsoft.AspNetCore", "Microsoft.Extensions");
    }

    [Fact]
    public void Application_DependsOnlyOnDomain_AndKnowsNothingAboutEfCoreOrMassTransit()
    {
        AssertDoesNotReference(Application, Infrastructure, Api);
        AssertDoesNotReferenceFrameworks(Application, "MassTransit", "Microsoft.EntityFrameworkCore", "Microsoft.AspNetCore");
    }

    [Fact]
    public void Infrastructure_DoesNotDependOnTheApi()
    {
        AssertDoesNotReference(Infrastructure, Api);
    }

    private static void AssertDoesNotReference(Assembly assembly, params Assembly[] forbidden)
    {
        var references = assembly.GetReferencedAssemblies().Select(a => a.Name).ToHashSet();

        foreach (var other in forbidden)
        {
            Assert.False(
                references.Contains(other.GetName().Name),
                $"{assembly.GetName().Name} must not depend on {other.GetName().Name}");
        }
    }

    private static void AssertDoesNotReferenceFrameworks(Assembly assembly, params string[] forbiddenPrefixes)
    {
        var offending = assembly.GetReferencedAssemblies()
            .Select(a => a.Name!)
            .Where(name => forbiddenPrefixes.Any(prefix => name.StartsWith(prefix, StringComparison.Ordinal)))
            .ToList();

        Assert.True(offending.Count == 0, $"{assembly.GetName().Name} must not depend on: {string.Join(", ", offending)}");
    }
}
