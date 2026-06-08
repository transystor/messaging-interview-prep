using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

var instanceName = args.FirstOrDefault() ?? $"retry-demo-{Environment.MachineName}";

var config = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
    GroupId = "orders-retry-demo",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    ClientId = instanceName
};

const string topic = "orders.created";

// Порог здесь чисто учебный: дорогие заказы считаем условно "проблемными".
// Это позволяет наглядно обсудить retry/DLQ, не городя сложную бизнес-валидацию.
const decimal failureThreshold = 1000m;

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(topic);

Console.WriteLine($"Kafka retry demo '{instanceName}' is listening. Orders with Amount > {failureThreshold} are marked for retry/DLQ explanation.");

while (true)
{
    var result = consumer.Consume(CancellationToken.None);
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

    if (order is null)
    {
        // Некорректный payload здесь просто пропускаем с commit.
        // Идея в том, чтобы показать: не каждую ошибку разумно крутить бесконечно,
        // иногда сообщение лучше сразу отправить в специальный error-flow.
        Console.WriteLine("[Kafka RetryDemo] invalid payload, skip with commit");
        consumer.Commit(result);
        continue;
    }

    if (order.Amount > failureThreshold)
    {
        Console.WriteLine($"[Kafka RetryDemo] order {order.OrderId} amount={order.Amount} would go to retry/DLQ flow");
        Console.WriteLine("[Kafka RetryDemo] in production you usually publish to retry topic or dead-letter topic, not just sleep forever in main consumer.");

        // В demo мы просто commit'им и логируем идею.
        // В реальной системе здесь обычно была бы публикация в retry topic или dead-letter topic.
        consumer.Commit(result);
        continue;
    }

    Console.WriteLine($"[Kafka RetryDemo] processed order {order.OrderId}, amount={order.Amount}");
    consumer.Commit(result);
}
