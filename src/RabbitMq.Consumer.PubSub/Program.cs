using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// Имя subscriber'а удобно передавать аргументом, чтобы в логах было видно,
// что один и тот же broadcast получают разные подписчики.
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

// queue: string.Empty просит RabbitMQ создать временную server-named очередь.
// Такой приём удобен для pub/sub demo: каждый subscriber получает свою собственную очередь,
// не мешает другим подписчикам и не требует ручного именования.
var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);

// Для fanout routing key не играет роли: exchange просто разошлёт копию сообщения во все связанные очереди.
await channel.QueueBindAsync(queue.QueueName, exchangeName, routingKey: string.Empty);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[RabbitMQ PubSub {subscriberName}] broadcast event: {json}");
    return Task.CompletedTask;
};

// autoAck:true здесь допустим, потому что пример учебный и бизнес-обработка фактически отсутствует.
// Для real-world критичной обработки чаще выбирают manual ack, как в work queue demo.
await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"RabbitMQ fanout subscriber '{subscriberName}' is listening. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
