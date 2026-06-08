using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

// Этот пример делает чуть больше, чем RetryDemo:
// при "проблемном" сообщении он не просто логирует идею, а реально публикует payload в отдельный dead-letter topic.
var instanceName = args.FirstOrDefault() ?? $"dlq-demo-{Environment.MachineName}";

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
    GroupId = "orders-dead-letter-demo",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    ClientId = instanceName
};

var producerConfig = new ProducerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
    ClientId = "kafka-dlq-demo-producer"
};

const string sourceTopic = "orders.created";
const string deadLetterTopic = "orders.created.dlq";
const decimal failureThreshold = 1000m;

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
consumer.Subscribe(sourceTopic);

Console.WriteLine($"Kafka DLQ demo '{instanceName}' is listening. Orders with Amount > {failureThreshold} are published to {deadLetterTopic}.");

while (true)
{
    var result = consumer.Consume(CancellationToken.None);
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

    if (order is null)
    {
        Console.WriteLine("[Kafka DLQ Demo] invalid payload, publish to DLQ");
        await PublishDeadLetterAsync(producer, deadLetterTopic, result.Message.Key ?? "invalid", result.Message.Value, "invalid-payload");
        consumer.Commit(result);
        continue;
    }

    if (order.Amount > failureThreshold)
    {
        Console.WriteLine($"[Kafka DLQ Demo] send order {order.OrderId} to DLQ because amount={order.Amount}");
        await PublishDeadLetterAsync(producer, deadLetterTopic, result.Message.Key, result.Message.Value, "amount-threshold");
        consumer.Commit(result);
        continue;
    }

    Console.WriteLine($"[Kafka DLQ Demo] processed order {order.OrderId}, amount={order.Amount}");
    consumer.Commit(result);
}

static async Task PublishDeadLetterAsync(IProducer<string, string> producer, string deadLetterTopic, string key, string payload, string reason)
{
    await producer.ProduceAsync(deadLetterTopic, new Message<string, string>
    {
        Key = key,
        Value = payload,
        Headers = new Headers
        {
            { "dead-letter-reason", System.Text.Encoding.UTF8.GetBytes(reason) }
        }
    });

    producer.Flush(TimeSpan.FromSeconds(5));
}
