namespace SharedModels;

public sealed record OrderCreatedEvent(
    Guid OrderId,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAtUtc,
    string Source);

public static class SampleData
{
    public static IReadOnlyList<OrderCreatedEvent> Orders { get; } =
    [
        new(Guid.NewGuid(), "cust-001", 1250m, DateTime.UtcNow, "checkout"),
        new(Guid.NewGuid(), "cust-002", 349m, DateTime.UtcNow, "mobile-app"),
        new(Guid.NewGuid(), "cust-003", 8990m, DateTime.UtcNow, "partner-api")
    ];
}
