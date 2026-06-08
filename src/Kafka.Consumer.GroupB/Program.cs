using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

var instanceName = args.FirstOrDefault() ?? $"group-b-{Environment.MachineName}";

var config = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",

    // Другой GroupId — принципиально важная часть примера.
    // Именно так Kafka показывает свою сильную сторону: один и тот же topic может независимо читать
    // и processing-группа, и analytics-группа, и любая другая downstream-система.
    GroupId = "orders-analytics-b",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    ClientId = instanceName
};

const string topic = "orders.created";

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(topic);

Console.WriteLine($"Kafka consumer '{instanceName}' in group '{config.GroupId}' is listening. Press Ctrl+C to stop.");

while (true)
{
    var result = consumer.Consume(CancellationToken.None);
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

    // Здесь consumer играет роль "аналитической" подсистемы.
    // Важно не то, что логика особенная, а то, что это независимая consumer group,
    // которая читает те же события, что и processing-group, но со своим собственным offset progress.
    Console.WriteLine($"[Kafka GroupB {instanceName}] analytics event for customer={result.Message.Key}, amount={order?.Amount}, offset={result.Offset.Value}");
    consumer.Commit(result);
}
