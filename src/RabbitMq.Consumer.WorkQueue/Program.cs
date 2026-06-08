using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedModels;

// Имя worker'а берём из аргументов, чтобы при запуске нескольких экземпляров легко видеть,
// как именно RabbitMQ распределяет сообщения между competing consumers.
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

// Объявляем ту же очередь, в которую пишет producer.
// В RabbitMQ это нормально: queue declaration можно делать и на стороне publisher, и на стороне consumer.
await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);

// prefetchCount=1 показывает одну из самых полезных настроек для work queue:
// не выдавать consumer'у новое сообщение, пока он не подтвердил предыдущее.
// Благодаря этому нагрузка распределяется более честно, особенно если обработка сообщений неравномерна по времени.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(json);

    Console.WriteLine($"[RabbitMQ Worker {workerName}] received order {order?.OrderId} from {order?.Source}");

    // Искусственная задержка нужна только для demo-эффекта.
    // Так проще увидеть, что при нескольких worker'ах сообщения действительно делятся между consumer'ами,
    // а не пролетают мгновенно через одного из них.
    await Task.Delay(TimeSpan.FromSeconds(2));
    Console.WriteLine($"[RabbitMQ Worker {workerName}] processed order {order?.OrderId}");

    // manual ack — ключевая часть work queue модели.
    // Пока ack не отправлен, RabbitMQ не считает сообщение успешно обработанным.
    // Если consumer упадёт до этого места, сообщение можно будет доставить заново.
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

// autoAck:false означает, что broker не должен автоматически считать сообщение обработанным.
// Мы подтверждаем обработку вручную через BasicAckAsync после завершения бизнес-логики.
await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine($"RabbitMQ worker '{workerName}' is listening. Press Ctrl+C to stop.");
await Task.Delay(Timeout.InfiniteTimeSpan);
