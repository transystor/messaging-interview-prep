namespace OutboxDemo.Api;

public sealed record CreateOrderRequest(
    string CustomerId,
    decimal Amount,
    string Source);
