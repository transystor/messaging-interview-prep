using System.Text.Json;
using Confluent.Kafka;
using SharedModels;

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
        Key = order.CustomerId,
        Value = payload,
        Headers = new Headers
        {
            { "message-type", System.Text.Encoding.UTF8.GetBytes(nameof(OrderCreatedEvent)) }
        }
    });

    Console.WriteLine($"[Kafka Producer] topic={result.Topic} partition={result.Partition.Value} offset={result.Offset.Value} key={order.CustomerId}");
    Console.WriteLine($"[Kafka Producer] payload={payload}");
}

producer.Flush(TimeSpan.FromSeconds(5));
Console.WriteLine("Kafka messages published.");
