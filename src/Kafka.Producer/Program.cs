using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

// ProducerConfig задаёт базовые transport-параметры producer'а.
// В учебной версии достаточно bootstrap server и client id,
// чтобы было видно минимально рабочую конфигурацию без лишнего operational шума.
var config = new ProducerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
    ClientId = "kafka-producer-demo"
};

const string topic = "orders.created";

using var producer = new ProducerBuilder<string, string>(config).Build();

foreach (var order in SampleData.Orders)
{
    var payload = JsonSerializer.Serialize(order);

    var result = await producer.ProduceAsync(topic, new Message<string, string>
    {
        // Key в Kafka важен не только как metadata.
        // Он влияет на partitioning: сообщения с одинаковым key обычно попадают в одну partition,
        // а значит внутри этого ключа проще сохранять порядок обработки.
        Key = order.CustomerId,
        Value = payload,
        Headers = new Headers
        {
            { "message-type", System.Text.Encoding.UTF8.GetBytes(nameof(OrderCreatedEvent)) }
        }
    });

    // Логируем partition и offset, потому что именно через них удобнее всего объяснять Kafka на собеседовании:
    // producer пишет в topic, broker кладёт запись в конкретную partition, а у записи появляется offset.
    Console.WriteLine($"[Kafka Producer] topic={result.Topic} partition={result.Partition.Value} offset={result.Offset.Value} key={order.CustomerId}");
    Console.WriteLine($"[Kafka Producer] payload={payload}");
}

// Flush нужен, чтобы дождаться фактической отправки накопленных сообщений перед завершением процесса.
producer.Flush(TimeSpan.FromSeconds(5));
Console.WriteLine("Kafka messages published.");
