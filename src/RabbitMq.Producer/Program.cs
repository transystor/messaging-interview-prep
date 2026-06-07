using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using SharedModels;

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

await channel.QueueDeclareAsync(queue: workQueueName, durable: true, exclusive: false, autoDelete: false);
await channel.ExchangeDeclareAsync(exchange: fanoutExchangeName, type: ExchangeType.Fanout, durable: true);
await channel.ExchangeDeclareAsync(exchange: topicExchangeName, type: ExchangeType.Topic, durable: true);

foreach (var order in SampleData.Orders)
{
    var json = JsonSerializer.Serialize(order);
    var body = Encoding.UTF8.GetBytes(json);

    var props = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        MessageId = order.OrderId.ToString(),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    };

    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: workQueueName, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] work queue <- {json}");

    await channel.BasicPublishAsync(exchange: fanoutExchangeName, routingKey: string.Empty, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] fanout exchange <- {json}");

    var routingKey = order.Amount > 1000m ? "order.error.payment" : "order.info.created";
    await channel.BasicPublishAsync(exchange: topicExchangeName, routingKey: routingKey, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] topic exchange ({routingKey}) <- {json}");
}

Console.WriteLine("RabbitMQ messages published.");
