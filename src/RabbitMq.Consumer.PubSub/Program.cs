using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var subscriberName = args.FirstOrDefault() ?? $"subscriber-{Guid.NewGuid():N}"[..18];

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

const string exchangeName = "orders.fanout";

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Fanout, durable: true);
var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);
await channel.QueueBindAsync(queue.QueueName, exchangeName, routingKey: string.Empty);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[RabbitMQ PubSub {subscriberName}] broadcast event: {json}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"RabbitMQ fanout subscriber '{subscriberName}' is listening. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
