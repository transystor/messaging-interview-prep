namespace Messaging.Api;

public sealed record CreateOrderRequest(
    string CustomerId,
    decimal Amount,
    string Source);
