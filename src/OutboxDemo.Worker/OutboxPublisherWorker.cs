using System.Text;
using Confluent.Kafka;
using Dapper;
using Npgsql;
using RabbitMQ.Client;

namespace OutboxDemo.Worker;

public sealed class OutboxPublisherWorker(ILogger<OutboxPublisherWorker> logger) : BackgroundService
{
    private readonly ILogger<OutboxPublisherWorker> _logger = logger;
    private readonly string _connectionString = Environment.GetEnvironmentVariable("OUTBOX_DB")
        ?? "Host=localhost;Port=5433;Database=outbox_demo;Username=postgres;Password=postgres";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox publisher worker started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PublishBatchAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox batch failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }

    private async Task PublishBatchAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        var messages = (await connection.QueryAsync<OutboxMessageRow>("""
            select id, message_type as MessageType, aggregate_id as AggregateId, payload, created_at_utc as CreatedAtUtc, published_at_utc as PublishedAtUtc, attempts
            from outbox_messages
            where published_at_utc is null
            order by created_at_utc
            limit 20
            """)).ToList();

        if (messages.Count == 0)
        {
            return;
        }

        foreach (var message in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await PublishToRabbitMqAsync(message.Payload);
                await PublishToKafkaAsync(message.AggregateId.ToString(), message.Payload);

                await connection.ExecuteAsync("""
                    update outbox_messages
                    set published_at_utc = now(), attempts = attempts + 1
                    where id = @Id
                    """, new { message.Id });

                _logger.LogInformation("Published outbox message {MessageId}", message.Id);
            }
            catch (Exception ex)
            {
                await connection.ExecuteAsync("""
                    update outbox_messages
                    set attempts = attempts + 1
                    where id = @Id
                    """, new { message.Id });

                _logger.LogWarning(ex, "Failed to publish outbox message {MessageId}", message.Id);
            }
        }
    }

    private static async Task PublishToRabbitMqAsync(string payload)
    {
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
        };

        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync("orders.work", durable: true, exclusive: false, autoDelete: false);
        await channel.ExchangeDeclareAsync("orders.fanout", ExchangeType.Fanout, durable: true);

        var body = Encoding.UTF8.GetBytes(payload);
        var props = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(string.Empty, "orders.work", false, props, body);
        await channel.BasicPublishAsync("orders.fanout", string.Empty, false, props, body);
    }

    private static async Task PublishToKafkaAsync(string key, string payload)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
            ClientId = "outbox-worker"
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        await producer.ProduceAsync("orders.created", new Message<string, string>
        {
            Key = key,
            Value = payload,
            Headers = new Confluent.Kafka.Headers
            {
                { "source", Encoding.UTF8.GetBytes("outbox-worker") }
            }
        });

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    private sealed record OutboxMessageRow(
        Guid Id,
        string MessageType,
        Guid AggregateId,
        string Payload,
        DateTime CreatedAtUtc,
        DateTime? PublishedAtUtc,
        int Attempts);
}
