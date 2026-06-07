# MessagingInterviewPrep

Учебный набор C#-примеров для подготовки к собеседованию по RabbitMQ и Kafka.

## С чего начать

Если хочешь проходить материал по порядку, открой сначала:

```text
docs/study-roadmap.md
```

А короткая шпаргалка для повторения лежит здесь:

```text
docs/interview-cheat-sheet.md
```

## Зачем этот репозиторий

Цель не просто "показать hello world", а руками увидеть:
- чем queue-based broker отличается от log-based broker
- как ведёт себя competing consumers в RabbitMQ
- чем pub/sub в RabbitMQ отличается от consumer groups в Kafka
- что такое durability, ack, offset, partition, routing, fanout
- как это выглядит в коде на C#

---

## Структура

- `src/SharedModels` — общий контракт сообщений
- `src/RabbitMq.Producer` — отправка сообщений в queue и fanout exchange
- `src/RabbitMq.Consumer.WorkQueue` — competing consumer для work queue
- `src/RabbitMq.Consumer.PubSub` — подписчик на fanout exchange
- `src/Kafka.Producer` — producer в topic `orders.created`
- `src/Kafka.Consumer.GroupA` — consumer group для "processing"
- `src/Kafka.Consumer.GroupB` — отдельная consumer group для "analytics"
- `src/RabbitMq.Consumer.TopicErrors` — подписчик на topic exchange c routing pattern
- `src/Kafka.Consumer.RetryDemo` — учебный consumer для объяснения retry / DLQ-подхода
- `docs/interview-cheat-sheet.md` — сжатая шпаргалка по вопросам/ответам
- `docs/study-roadmap.md` — маршрут изучения по шагам
- `src/Messaging.Api` — ASP.NET Core minimal API, которая публикует событие и в RabbitMQ, и в Kafka
- `src/OutboxDemo.Api` — API, которая пишет заказ и событие в PostgreSQL через transactional outbox
- `src/OutboxDemo.Worker` — worker, который читает outbox table и публикует события в брокеры

---

## Быстрый старт

### 1. Поднять инфраструктуру

```bash
docker compose up -d
```

Это поднимет:
- RabbitMQ на `localhost:5672`
- RabbitMQ Management UI на `http://localhost:15672`
- Kafka на `localhost:9092`
- PostgreSQL для outbox demo на `localhost:5433`

### 2. Сборка

```bash
dotnet build MessagingInterviewPrep.sln
```

### 3. Запуск demo API

```bash
dotnet run --project src/Messaging.Api
```

Пример запроса:

```bash
curl -X POST http://localhost:5137/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-777",
    "amount": 2450,
    "source": "interview-demo"
  }'
```

Что происходит:
- API принимает HTTP request
- формирует `OrderCreatedEvent`
- публикует его в RabbitMQ:
  - work queue
  - fanout exchange
  - topic exchange
- публикует тот же event в Kafka topic `orders.created`

---

# RabbitMQ

## Идея RabbitMQ

RabbitMQ, в типичном interview-контексте, это broker, который хорошо подходит для:
- task distribution
- routing
- asynchronous integration
- "обработай задачу один раз одним worker'ом"

RabbitMQ мыслит категориями:
- producer
- exchange
- queue
- consumer
- binding
- ack

Ключевая идея: **сообщение обычно маршрутизируется в queue, а потом один из consumers его забирает**.

---

## Пример 1. Work Queue

### Что демонстрирует

`RabbitMq.Producer` кладёт сообщения в очередь:
- `orders.work`

`RabbitMq.Consumer.WorkQueue` читает их с manual ack и `prefetch = 1`.

### Как запускать

Окно 1:
```bash
dotnet run --project src/RabbitMq.Consumer.WorkQueue worker-1
```

Окно 2:
```bash
dotnet run --project src/RabbitMq.Consumer.WorkQueue worker-2
```

Окно 3:
```bash
dotnet run --project src/RabbitMq.Producer
```

### Что увидеть

Сообщения будут делиться между `worker-1` и `worker-2`.

Это и есть **competing consumers**:
- одна очередь
- несколько workers
- каждое конкретное сообщение обрабатывает только один worker

### Что важно для собеса

#### `QueueDeclare`
Создаёт очередь, если её ещё нет.

#### `durable: true`
Очередь переживает restart broker'а.

#### `Persistent = true`
Сообщение помечается как persistent.

#### `BasicAck`
Consumer вручную подтверждает успешную обработку.

Если consumer умер **до ack**, RabbitMQ сможет отдать сообщение другому worker'у.

#### `BasicQos(prefetchCount: 1)`
Не давать одному worker'у слишком много unacked сообщений сразу.
Это важный механизм fair dispatch.

---

## Пример 2. Pub/Sub через Fanout Exchange

### Что демонстрирует

Producer также публикует события в:
- exchange `orders.fanout`

`RabbitMq.Consumer.PubSub` создаёт временную очередь и подписывается на fanout exchange.

### Как запускать

Окно 1:
```bash
dotnet run --project src/RabbitMq.Consumer.PubSub audit
```

Окно 2:
```bash
dotnet run --project src/RabbitMq.Consumer.PubSub notifications
```

Окно 3:
```bash
dotnet run --project src/RabbitMq.Producer
```

### Что увидеть

Каждый подписчик получит **копию** каждого сообщения.

Это уже не competing consumers, а **broadcast**.

### Что важно для собеса

#### Exchange
Producer в RabbitMQ обычно публикует не напрямую в consumer, а в exchange.

#### Fanout exchange
Игнорирует routing key и шлёт сообщение во все связанные очереди.

#### Временная exclusive queue
Удобна для временного subscriber'а, которому не нужна постоянная очередь.

---

## Пример 3. Topic Exchange

### Что демонстрирует

Producer также публикует события в:
- `orders.topic`

Routing key выбирается так:
- дорогой заказ: `order.error.payment`
- обычный заказ: `order.info.created`

`RabbitMq.Consumer.TopicErrors` подписывается на pattern, например:
- `order.error.*`

### Как запускать

Окно 1:
```bash
dotnet run --project src/RabbitMq.Consumer.TopicErrors billing-errors order.error.*
```

Окно 2:
```bash
dotnet run --project src/RabbitMq.Producer
```

### Что важно для собеса

`topic exchange` позволяет гибче routing, чем `fanout`:
- `*` совпадает с одним сегментом
- `#` совпадает с несколькими сегментами

Это удобно для selective subscription по типу событий.

---

## RabbitMQ, как объяснять на собеседовании

Хорошая формулировка:

> RabbitMQ удобен, когда нужен классический message broker с явными очередями, гибким routing и понятной моделью task processing. Особенно хорош для work queues, background jobs, integration between services и сценариев, где одно сообщение обычно должен обработать один consumer.

Ещё важно сказать, что RabbitMQ:
- не про долгую историю сообщений как основную модель
- не про replay как ключевую фичу
- обычно используется как "операционный broker"

---

# Kafka

## Идея Kafka

Kafka, в interview-контексте, это distributed append-only log.

Kafka мыслит категориями:
- topic
- partition
- offset
- producer
- consumer group
- retention

Ключевая идея: **producer пишет запись в log, а consumers читают log по offsets**.

Сообщение не "исчезает" сразу после чтения одним consumer'ом. Разные consumer groups могут читать его независимо.

---

## Пример 1. Producer

`Kafka.Producer` пишет события в topic:
- `orders.created`

В качестве key используется `CustomerId`.

### Почему это важно

Key влияет на partitioning.
Если одинаковый key, Kafka старается отправлять сообщения в одну и ту же partition, чтобы сохранить порядок в рамках key.

---

## Пример 2. Consumer Group A

`Kafka.Consumer.GroupA` работает как группа обработки заказов:
- `GroupId = orders-processors-a`

Если запустить два экземпляра этого проекта с одним `GroupId`, Kafka будет балансировать partitions между ними.

### Важно
Это не совсем то же самое, что RabbitMQ competing consumers, хотя внешне похоже.

В Kafka распределяются **partitions**, а не отдельные сообщения абстрактно.
Порядок гарантируется внутри partition.

---

## Пример 3. Consumer Group B

`Kafka.Consumer.GroupB` использует другой `GroupId`:
- `orders-analytics-b`

Она читает те же события независимо от Group A.

### Что демонстрирует

Одно и то же событие может быть:
- обработано processing-group
- отдельно обработано analytics-group

И это нормальная, нативная модель Kafka.

---

## Как запускать Kafka-примеры

Окно 1:
```bash
dotnet run --project src/Kafka.Consumer.GroupA processor-1
```

Окно 2:
```bash
dotnet run --project src/Kafka.Consumer.GroupB analytics-1
```

Окно 3:
```bash
dotnet run --project src/Kafka.Producer
```

Для демонстрации масштабирования внутри одной группы можно запустить ещё один consumer группы A:

```bash
dotnet run --project src/Kafka.Consumer.GroupA processor-2
```

---

## Пример 4. Retry / Dead Letter концепция

`Kafka.Consumer.RetryDemo` специально показывает учебный сценарий:
- если заказ дорогой, обычная обработка помечает его как кандидат в retry/DLQ flow

Здесь логика упрощённая, но interview-смысл важный:
- в Kafka обычно не любят бесконечно держать проблемное сообщение в основном consumer loop
- вместо этого используют retry topics и dead-letter topics

### Как запускать

Окно 1:
```bash
dotnet run --project src/Kafka.Consumer.RetryDemo
```

Окно 2:
```bash
dotnet run --project src/Kafka.Producer
```

---

## Что важно для собеса по Kafka

#### Topic
Логическая категория сообщений.

#### Partition
Физическое/логическое деление topic для параллелизма и масштабирования.

#### Offset
Позиция сообщения внутри partition.

#### Consumer Group
Механизм независимого чтения и масштабирования.

#### Commit offset
Consumer фиксирует, до какого места он дочитал.

#### Retention
Сообщения живут заданное время или пока не достигнут лимит размера, даже если кто-то их уже прочитал.

---

## Kafka, как объяснять на собеседовании

Хорошая формулировка:

> Kafka полезна, когда нужен высокопроизводительный event streaming, независимое чтение одних и тех же событий разными системами, replay истории, retention и масштабирование через partitions и consumer groups.

Ещё важно сказать, что Kafka хорошо подходит для:
- event-driven architecture
- audit/event history
- analytics pipelines
- integration streams
- high-throughput async systems

---

# Главное отличие RabbitMQ и Kafka

## RabbitMQ
Обычно думаешь так:
- есть очередь
- кто-то забрал задачу
- задача считается доставленной конкретному обработчику

## Kafka
Обычно думаешь так:
- есть поток событий в log
- разные группы читают его независимо
- сообщения хранятся по retention policy

---

## Простая interview-шпаргалка

### Когда RabbitMQ
- background jobs
- task queues
- routing по exchange/binding
- request/async processing
- нужно быстро и явно распределять задачи между workers

### Когда Kafka
- event streaming
- event sourcing adjacent patterns
- аналитика
- replay
- несколько независимых consumer groups
- большие объёмы событий

---

# На что обратить внимание в коде

## RabbitMQ код
Смотри на:
- `QueueDeclareAsync`
- `ExchangeDeclareAsync`
- `QueueBindAsync`
- `BasicPublishAsync`
- `BasicConsumeAsync`
- `BasicAckAsync`
- `BasicQosAsync`

## Kafka код
Смотри на:
- `ProducerBuilder`
- `ProduceAsync`
- `ConsumerBuilder`
- `Subscribe`
- `Consume`
- `Commit`
- `GroupId`
- `AutoOffsetReset`

---

# Хорошие устные ответы для собеседования

## Вопрос: чем RabbitMQ отличается от Kafka?

Короткий сильный ответ:

> RabbitMQ это классический broker очередей. Он удобен для task distribution и routing. Kafka это distributed log для event streams. В Kafka сообщения читаются по offsets, могут долго храниться и переиспользоваться разными consumer groups независимо друг от друга.

## Вопрос: что такое consumer group в Kafka?

> Это набор consumers с одним GroupId. Kafka распределяет partitions topic между ними, чтобы внутри группы каждое сообщение читалось одним логическим потребителем, а разные группы могли читать тот же topic независимо.

## Вопрос: зачем manual ack в RabbitMQ?

> Чтобы broker не считал сообщение обработанным раньше времени. Если consumer падает до ack, сообщение можно доставить повторно другому worker'у.

## Вопрос: зачем message key в Kafka?

> Key влияет на partitioning. Это позволяет сохранять порядок событий для одного business key, например для одного пользователя или заказа.

---

# Transactional Outbox demo

`src/OutboxDemo.Api` и `src/OutboxDemo.Worker` показывают один из самых полезных interview-паттернов.

## Проблема

Плохой наивный сценарий выглядит так:
1. сохранить заказ в БД
2. потом сразу опубликовать событие в broker

Если между шагом 1 и шагом 2 процесс упал, получится рассинхрон:
- заказ в БД уже есть
- события в broker нет

## Идея outbox pattern

Правильнее сделать так:
1. в одной транзакции записать:
   - сам заказ
   - запись в `outbox_messages`
2. отдельный worker читает `outbox_messages`
3. публикует события в broker
4. помечает запись как опубликованную

Так мы уменьшаем риск потери событий между БД и messaging layer.

## Что есть в demo

### `OutboxDemo.Api`
- `POST /orders`
- пишет заказ в таблицу `orders`
- в той же транзакции пишет событие в `outbox_messages`

### `OutboxDemo.Worker`
- периодически читает неопубликованные outbox записи
- публикует их в RabbitMQ и Kafka
- ставит `published_at_utc`
- увеличивает `attempts`

## Как запускать

Окно 1:
```bash
docker compose up -d
```

Окно 2:
```bash
dotnet run --project src/OutboxDemo.Api
```

Окно 3:
```bash
dotnet run --project src/OutboxDemo.Worker
```

Окно 4:
```bash
curl -X POST http://localhost:5105/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-outbox-1",
    "amount": 1999,
    "source": "outbox-demo"
  }'
```

Потом можно посмотреть outbox:

```bash
curl http://localhost:5105/outbox
```

## Что говорить на собеседовании

Хорошая формулировка:

> Transactional outbox нужен, чтобы не потерять событие в промежутке между коммитом бизнес-данных в БД и публикацией в broker. Мы сначала сохраняем и данные, и outbox-запись в одной локальной транзакции, а уже отдельный publisher/worker надёжно вычитывает и отправляет события дальше.

Важно честно сказать: outbox обычно даёт **at-least-once delivery**, поэтому consumer'ы должны быть готовы к идемпотентной обработке.

---

# End-to-end ASP.NET Core пример

`src/Messaging.Api` это учебный сценарий, близкий к реальному backend flow:

- клиент делает `POST /orders`
- API валидирует входные данные
- создаёт доменное событие `OrderCreatedEvent`
- публикует его в RabbitMQ и Kafka

## Зачем это полезно для собеседования

На собесе часто важно показать не только знание терминов, но и понимание реального integration path:

- где рождается событие
- как API связано с broker'ом
- как одно и то же бизнес-событие может потребляться разными downstream системами

Хорошая устная формулировка:

> HTTP API принимает команду от клиента, формирует бизнес-событие и публикует его в messaging infrastructure. Дальше разные consumers могут независимо обрабатывать это событие, например operational workers через RabbitMQ и analytics/stream consumers через Kafka.

---

# Interview cheat sheet

Отдельная шпаргалка лежит здесь:

```text
docs/interview-cheat-sheet.md
```

Там короткие сильные формулировки для типовых вопросов.

---

# Что можно сделать следующим этапом

Я бы дальше добавила ещё 2 полезных учебных сценария:
1. ASP.NET Core API, которая публикует события в RabbitMQ и Kafka
2. Outbox pattern на C# с PostgreSQL

Это уже даст почти полноценный interview-lab уровня middle/senior backend.
