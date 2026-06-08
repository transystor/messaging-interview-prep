using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

var instanceName = args.FirstOrDefault() ?? $"group-a-{Environment.MachineName}";

var config = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",

    // GroupId определяет логического потребителя в терминах Kafka.
    // Если поднять два процесса с одним и тем же GroupId, Kafka будет распределять partitions между ними,
    // а не отдавать обеим копию одного и того же потока.
    GroupId = "orders-processors-a",

    // Earliest полезен для учебного demo: даже если consumer стартует после producer'а,
    // он всё равно прочитает старые сообщения из topic, а не только новые.
    AutoOffsetReset = AutoOffsetReset.Earliest,

    // Отключаем auto-commit, чтобы явно показывать момент, когда приложение считает сообщение обработанным.
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

    Console.WriteLine($"[Kafka GroupA {instanceName}] partition={result.Partition.Value} offset={result.Offset.Value} customer={result.Message.Key} order={order?.OrderId}");

    // Commit фиксирует progress consumer'а в рамках consumer group.
    // Идея похожа на ack по смыслу надёжности, но модель другая: Kafka не удаляет сообщение,
    // а просто запоминает, до какого offset эта группа уже дошла.
    consumer.Commit(result);
}
