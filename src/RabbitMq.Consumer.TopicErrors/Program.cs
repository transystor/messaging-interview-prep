using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// subscriberName нужен только для читаемых логов.
// routingPattern — это уже содержательная часть примера: он показывает, как topic exchange
// позволяет получать не все события подряд, а только подходящие под pattern.
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

// Как и в fanout demo, создаём временную очередь на каждый запуск subscriber'а.
// Это позволяет быстро экспериментировать с разными routing patterns без ручной подготовки topology.
var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);

// В topic exchange routingPattern уже критичен.
// Пример "order.error.*" поймает события вроде order.error.payment,
// но не поймает order.info.created.
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
