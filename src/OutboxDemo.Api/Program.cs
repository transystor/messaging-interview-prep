using System.Data;
using System.Text.Json;
using Dapper;
using Npgsql;
using OutboxDemo.Api;
using SharedModels;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var connectionString = Environment.GetEnvironmentVariable("OUTBOX_DB")
    ?? "Host=localhost;Port=5433;Database=outbox_demo;Username=postgres;Password=postgres";

await EnsureSchemaAsync(connectionString);

app.MapGet("/", () => Results.Ok(new
{
    service = "OutboxDemo.Api",
    endpoints = new[]
    {
        "POST /orders",
        "GET /outbox"
    }
}));

app.MapGet("/outbox", async () =>
{
    await using var connection = new NpgsqlConnection(connectionString);
    var rows = await connection.QueryAsync("""
        select id, message_type as MessageType, aggregate_id as AggregateId, payload, created_at_utc as CreatedAtUtc, published_at_utc as PublishedAtUtc
        from outbox_messages
        order by created_at_utc desc
        limit 20
        """);

    return Results.Ok(rows);
});

app.MapPost("/orders", async (CreateOrderRequest request) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerId))
    {
        return Results.BadRequest(new { error = "CustomerId is required" });
    }

    if (string.IsNullOrWhiteSpace(request.Source))
    {
        return Results.BadRequest(new { error = "Source is required" });
    }

    if (request.Amount <= 0)
    {
        return Results.BadRequest(new { error = "Amount must be > 0" });
    }

    var order = new OrderCreatedEvent(
        Guid.NewGuid(),
        request.CustomerId,
        request.Amount,
        DateTime.UtcNow,
        request.Source);

    var payload = JsonSerializer.Serialize(order);

    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    await using var transaction = await connection.BeginTransactionAsync();

    await connection.ExecuteAsync("""
        insert into orders (id, customer_id, amount, source, created_at_utc)
        values (@Id, @CustomerId, @Amount, @Source, @CreatedAtUtc)
        """, new
    {
        Id = order.OrderId,
        order.CustomerId,
        order.Amount,
        order.Source,
        order.CreatedAtUtc
    }, transaction);

    await connection.ExecuteAsync("""
        insert into outbox_messages (id, message_type, aggregate_id, payload, created_at_utc)
        values (@Id, @MessageType, @AggregateId, @Payload, @CreatedAtUtc)
        """, new
    {
        Id = Guid.NewGuid(),
        MessageType = nameof(OrderCreatedEvent),
        AggregateId = order.OrderId,
        Payload = payload,
        CreatedAtUtc = DateTime.UtcNow
    }, transaction);

    await transaction.CommitAsync();

    return Results.Ok(new
    {
        message = "Order saved. Event stored in outbox.",
        order
    });
});

app.Run();

static async Task EnsureSchemaAsync(string connectionString)
{
    const string sql = """
        create table if not exists orders
        (
            id uuid primary key,
            customer_id text not null,
            amount numeric(18,2) not null,
            source text not null,
            created_at_utc timestamp with time zone not null
        );

        create table if not exists outbox_messages
        (
            id uuid primary key,
            message_type text not null,
            aggregate_id uuid not null,
            payload jsonb not null,
            created_at_utc timestamp with time zone not null,
            published_at_utc timestamp with time zone null,
            attempts integer not null default 0
        );

        create index if not exists ix_outbox_messages_unpublished
            on outbox_messages (published_at_utc, created_at_utc);
        """;

    await using var connection = new NpgsqlConnection(connectionString);
    await connection.ExecuteAsync(sql);
}
