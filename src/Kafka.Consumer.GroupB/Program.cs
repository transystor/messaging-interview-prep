using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

var instanceName = args.FirstOrDefault() ?? $"group-b-{Environment.MachineName}";

var config = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
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

    Console.WriteLine($"[Kafka GroupB {instanceName}] analytics event for customer={result.Message.Key}, amount={order?.Amount}, offset={result.Offset.Value}");
    consumer.Commit(result);
}
