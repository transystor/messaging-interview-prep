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
                // Ошибки батча логируем, но не убиваем процесс.
                // Это типичный подход для background publisher'а: проблема одной итерации не должна ронять весь worker.
                _logger.LogError(ex, "Outbox batch failed");
            }

            // Небольшая пауза между опросами таблицы нужна, чтобы worker не крутил tight loop без пользы.
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }

    private async Task PublishBatchAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);

        // Берём только те сообщения, которые ещё не были опубликованы.
        // Сортировка по created_at_utc помогает сохранять более естественный порядок публикации.
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
                // Один outbox record публикуем сразу в оба transport'а.
                // В production так делают только если это действительно часть архитектуры,
                // а здесь это полезно для учебного сравнения RabbitMQ и Kafka на одном и том же event payload.
                await PublishToRabbitMqAsync(message.Payload);
                await PublishToKafkaAsync(message.AggregateId.ToString(), message.Payload);

                // published_at_utc — основной признак, что сообщение успешно покинуло outbox table.
                await connection.ExecuteAsync("""
                    update outbox_messages
                    set published_at_utc = now(), attempts = attempts + 1
                    where id = @Id
                    """, new { message.Id });

                _logger.LogInformation("Published outbox message {MessageId}", message.Id);
            }
            catch (Exception ex)
            {
                // attempts увеличиваем даже на неуспехе, чтобы видеть повторные попытки и иметь базу для retry policy.
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

        // В outbox demo RabbitMQ-публикация намеренно проще, чем в основном API-примере.
        // Наша главная цель здесь не routing-фичи, а сам факт надёжной отложенной публикации из таблицы outbox.
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

    // Отдельный record для чтения строки outbox-таблицы.
    // Мы явно храним published_at и attempts, потому что именно эти поля чаще всего нужны
    // для operational-логики фонового publisher'а.
    private sealed record OutboxMessageRow(
        Guid Id,
        string MessageType,
        Guid AggregateId,
        string Payload,
        DateTime CreatedAtUtc,
        DateTime? PublishedAtUtc,
        int Attempts);
}
