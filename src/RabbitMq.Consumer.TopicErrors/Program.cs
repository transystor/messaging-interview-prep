using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var subscriberName = args.FirstOrDefault() ?? "billing-errors";
var routingPattern = args.Skip(1).FirstOrDefault() ?? "order.error.*";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

const string exchangeName = "orders.topic";

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic, durable: true);
var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);
await channel.QueueBindAsync(queue.QueueName, exchangeName, routingPattern);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var payload = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[RabbitMQ Topic {subscriberName}] routingKey={ea.RoutingKey}, payload={payload}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"RabbitMQ topic subscriber '{subscriberName}' listens pattern '{routingPattern}'. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
