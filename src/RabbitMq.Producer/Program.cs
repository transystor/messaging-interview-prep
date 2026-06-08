using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using SharedModels;

// ConnectionFactory задаёт базовые параметры подключения к RabbitMQ.
// Для учебного стенда читаем их из env, но оставляем безопасные локальные default-значения,
// чтобы пример запускался без дополнительной настройки.
var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD") ?? "guest"
};

// Здесь специально заведены три независимых RabbitMQ-сценария на одном наборе событий:
// 1. work queue        -> competing consumers
// 2. fanout exchange   -> broadcast pub/sub
// 3. topic exchange    -> selective routing по routing key
const string workQueueName = "orders.work";
const string fanoutExchangeName = "orders.fanout";
const string topicExchangeName = "orders.topic";
const string directExchangeName = "orders.direct";

var connection = await factory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

// durable:true означает, что queue/exchange переживут restart broker'а.
// В учебном коде это полезно, чтобы сразу показать best-practice для non-ephemeral сообщений.
await channel.QueueDeclareAsync(queue: workQueueName, durable: true, exclusive: false, autoDelete: false);
await channel.ExchangeDeclareAsync(exchange: fanoutExchangeName, type: ExchangeType.Fanout, durable: true);
await channel.ExchangeDeclareAsync(exchange: topicExchangeName, type: ExchangeType.Topic, durable: true);
await channel.ExchangeDeclareAsync(exchange: directExchangeName, type: ExchangeType.Direct, durable: true);

foreach (var order in SampleData.Orders)
{
    // Для простоты transport-уровня всё сериализуем в JSON.
    // Так легче читать payload и в логах, и в management UI.
    var json = JsonSerializer.Serialize(order);
    var body = Encoding.UTF8.GetBytes(json);

    // BasicProperties несут transport metadata.
    // Здесь важнее всего Persistent=true: вместе с durable queue это приближает нас к более надёжной доставке.
    var props = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        MessageId = order.OrderId.ToString(),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    };

    // Публикация в default exchange с routingKey = имя очереди.
    // Это самый прямой путь показать классическую work queue модель RabbitMQ.
    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: workQueueName, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] work queue <- {json}");

    // Fanout exchange игнорирует routing key и просто рассылает копию события во все связанные очереди.
    await channel.BasicPublishAsync(exchange: fanoutExchangeName, routingKey: string.Empty, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] fanout exchange <- {json}");

    // Для topic exchange выбираем routing key на основе бизнес-условия.
    // Это позволяет потом показать, как subscribers selectively подписываются только на интересующие типы событий.
    var routingKey = order.Amount > 1000m ? "order.error.payment" : "order.info.created";
    await channel.BasicPublishAsync(exchange: topicExchangeName, routingKey: routingKey, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] topic exchange ({routingKey}) <- {json}");

    // Для direct exchange используем точные routing keys без pattern matching.
    // Это полезно, когда маршрут определяется небольшой конечной категорией: billing, shipping, email и т.п.
    var directRoutingKey = order.Amount > 1000m ? "billing" : "shipping";
    await channel.BasicPublishAsync(exchange: directExchangeName, routingKey: directRoutingKey, mandatory: false, basicProperties: props, body: body);
    Console.WriteLine($"[RabbitMQ Producer] direct exchange ({directRoutingKey}) <- {json}");
}

Console.WriteLine("RabbitMQ messages published.");
