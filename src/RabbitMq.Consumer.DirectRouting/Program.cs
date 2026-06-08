using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// Direct exchange удобен, когда маршрутизация строится по точному совпадению ключа.
// В отличие от topic exchange, здесь нет wildcard pattern matching: binding key должен совпасть ровно.
var subscriberName = args.FirstOrDefault() ?? "billing-service";
var bindingKey = args.Skip(1).FirstOrDefault() ?? "billing";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

const string exchangeName = "orders.direct";

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);

// Каждому subscriber'у даём временную очередь, чтобы можно было быстро запускать несколько routing-примеров
// без ручного управления topology.
var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);
await channel.QueueBindAsync(queue.QueueName, exchangeName, bindingKey);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var payload = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[RabbitMQ Direct {subscriberName}] bindingKey={bindingKey}, routingKey={ea.RoutingKey}, payload={payload}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"RabbitMQ direct subscriber '{subscriberName}' listens exact key '{bindingKey}'. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
