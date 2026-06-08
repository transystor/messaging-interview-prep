namespace SharedModels;

// Общий контракт события, который используется и в RabbitMQ-примерах, и в Kafka-примерах.
// Идея намеренно простая: одно и то же бизнес-событие проходит через разные transport-механизмы,
// чтобы было легче сравнивать поведение брокеров, а не путаться в разных моделях данных.
public sealed record OrderCreatedEvent(
    Guid OrderId,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAtUtc,
    string Source);

public static class SampleData
{
    // Готовый набор тестовых событий для локальных demo-сценариев.
    // Мы держим данные прямо в коде, потому что цель проекта учебная: важно быстро запускать примеры,
    // не отвлекаясь на отдельную БД или генератор тестовых данных.
    public static IReadOnlyList<OrderCreatedEvent> Orders { get; } =
    [
        new(Guid.NewGuid(), "cust-001", 1250m, DateTime.UtcNow, "checkout"),
        new(Guid.NewGuid(), "cust-002", 349m, DateTime.UtcNow, "mobile-app"),
        new(Guid.NewGuid(), "cust-003", 8990m, DateTime.UtcNow, "partner-api")
    ];
}
