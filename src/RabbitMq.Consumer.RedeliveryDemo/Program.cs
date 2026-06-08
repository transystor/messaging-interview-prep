using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedModels;

// Этот пример показывает redelivery максимально прямо:
// consumer получает сообщение, первый раз намеренно не ack'ает его и делает requeue,
// а при повторной доставке уже успешно завершает обработку.
var workerName = args.FirstOrDefault() ?? "redelivery-demo";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

const string queueName = "orders.work";
var firstAttemptFailures = new HashSet<string>();

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(json);
    var messageId = ea.BasicProperties.MessageId ?? order?.OrderId.ToString() ?? "unknown";

    Console.WriteLine($"[RabbitMQ Redelivery {workerName}] received messageId={messageId}, redelivered={ea.Redelivered}");

    // Симулируем падение/ошибку на первой попытке.
    // Вместо ack делаем BasicNack с requeue=true, чтобы RabbitMQ снова поставил сообщение в очередь.
    if (!firstAttemptFailures.Contains(messageId))
    {
        firstAttemptFailures.Add(messageId);
        Console.WriteLine($"[RabbitMQ Redelivery {workerName}] simulate failure for messageId={messageId}, requeue=true");
        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
        return;
    }

    await Task.Delay(TimeSpan.FromSeconds(1));
    Console.WriteLine($"[RabbitMQ Redelivery {workerName}] processed on retry messageId={messageId}");
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine($"RabbitMQ redelivery demo '{workerName}' is listening. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
