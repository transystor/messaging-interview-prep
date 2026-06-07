using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Messaging.Api;
using RabbitMQ.Client;
using SharedModels;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "Messaging.Api",
    endpoints = new[]
    {
        "POST /orders"
    }
}));

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
        OrderId: Guid.NewGuid(),
        CustomerId: request.CustomerId,
        Amount: request.Amount,
        CreatedAtUtc: DateTime.UtcNow,
        Source: request.Source);

    var json = JsonSerializer.Serialize(order);

    await PublishToRabbitMqAsync(order, json);
    await PublishToKafkaAsync(order, json);

    return Results.Ok(new
    {
        message = "Order accepted and published to RabbitMQ + Kafka",
        order
    });
});

app.Run();

static async Task PublishToRabbitMqAsync(OrderCreatedEvent order, string json)
{
    var factory = new ConnectionFactory
    {
        HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
        UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
        Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
    };

    const string workQueueName = "orders.work";
    const string fanoutExchangeName = "orders.fanout";
    const string topicExchangeName = "orders.topic";

    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.QueueDeclareAsync(workQueueName, durable: true, exclusive: false, autoDelete: false);
    await channel.ExchangeDeclareAsync(fanoutExchangeName, ExchangeType.Fanout, durable: true);
    await channel.ExchangeDeclareAsync(topicExchangeName, ExchangeType.Topic, durable: true);

    var body = Encoding.UTF8.GetBytes(json);
    var props = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        MessageId = order.OrderId.ToString(),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    };

    await channel.BasicPublishAsync(string.Empty, workQueueName, false, props, body);
    await channel.BasicPublishAsync(fanoutExchangeName, string.Empty, false, props, body);

    var routingKey = order.Amount > 1000m ? "order.error.payment" : "order.info.created";
    await channel.BasicPublishAsync(topicExchangeName, routingKey, false, props, body);
}

static async Task PublishToKafkaAsync(OrderCreatedEvent order, string json)
{
    var config = new ProducerConfig
    {
        BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
        ClientId = "messaging-api"
    };

    using var producer = new ProducerBuilder<string, string>(config).Build();
    await producer.ProduceAsync("orders.created", new Message<string, string>
    {
        Key = order.CustomerId,
        Value = json,
        Headers = new Confluent.Kafka.Headers
        {
            { "message-type", Encoding.UTF8.GetBytes(nameof(OrderCreatedEvent)) },
            { "source", Encoding.UTF8.GetBytes(order.Source) }
        }
    });

    producer.Flush(TimeSpan.FromSeconds(5));
}
