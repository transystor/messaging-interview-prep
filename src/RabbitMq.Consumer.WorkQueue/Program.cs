using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedModels;

var workerName = args.FirstOrDefault() ?? Environment.MachineName;

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

const string queueName = "orders.work";

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(json);

    Console.WriteLine($"[RabbitMQ Worker {workerName}] received order {order?.OrderId} from {order?.Source}");
    await Task.Delay(TimeSpan.FromSeconds(2));
    Console.WriteLine($"[RabbitMQ Worker {workerName}] processed order {order?.OrderId}");

    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine($"RabbitMQ worker '{workerName}' is listening. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
