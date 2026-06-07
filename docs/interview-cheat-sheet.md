# Kafka и RabbitMQ, interview cheat sheet

## 1. В чём главная разница?

### RabbitMQ
- классический message broker
- мыслит очередями, exchange, routing
- хорош для task distribution
- сообщение обычно должен обработать один consumer

### Kafka
- distributed append-only log
- мыслит topic, partition, offset, consumer group
- хорош для event streaming
- одно и то же событие могут независимо читать разные consumer groups

Короткая формулировка:

> RabbitMQ чаще про доставку задач и routing, Kafka чаще про поток событий, retention и независимое чтение истории.

---

## 2. Когда выбирать RabbitMQ?

Выбирай RabbitMQ, когда нужно:
- background jobs
- work queues
- routing по direct/topic/fanout exchange
- fairly simple async integration между сервисами
- быстро доставить задачу worker'у

---

## 3. Когда выбирать Kafka?

Выбирай Kafka, когда нужно:
- event streaming
- high throughput
- replay событий
- несколько независимых downstream consumers
- аналитика и event pipelines
- retention истории событий

---

## 4. Что такое ack в RabbitMQ?

`ack` это подтверждение от consumer, что сообщение реально обработано.

Если consumer падает до `ack`, broker может отдать сообщение снова.

Это важно, потому что без manual ack можно потерять задачу при падении consumer'а.

---

## 5. Что такое offset в Kafka?

Offset это позиция сообщения внутри partition.

Consumer хранит прогресс чтения через committed offsets.

Kafka не удаляет сообщение просто потому, что один consumer его прочитал.

---

## 6. Что такое consumer group в Kafka?

Consumer group это набор consumers с одним `GroupId`.

Внутри группы partitions распределяются между consumers, чтобы:
- масштабировать обработку
- не читать одно и то же сообщение дважды внутри одной группы

Но другая группа может читать тот же topic независимо.

---

## 7. Почему Kafka не то же самое, что RabbitMQ queue?

Потому что модель разная.

### RabbitMQ
- сообщение попадает в queue
- broker доставляет его consumer'у
- после успешной обработки оно в operational sense считается обработанным

### Kafka
- сообщение лежит в log
- consumer читает по offset
- другие consumer groups могут читать тот же event независимо

---

## 8. Что такое partition key в Kafka?

Key влияет на partitioning.

Если одинаковый key, Kafka обычно держит порядок для этого key в пределах одной partition.

Типичный пример:
- key = `CustomerId`
- тогда события одного клиента читаются в правильном порядке

---

## 9. Что такое prefetch в RabbitMQ?

`prefetch` ограничивает число unacked сообщений у consumer.

Например `prefetch = 1` помогает fair dispatch:
- не заливать одного worker'а задачами
- равномернее распределять нагрузку

---

## 10. Что такое DLQ / retry?

### RabbitMQ
Обычно используют:
- dead-letter exchange
- TTL
- retry queues

### Kafka
Обычно используют:
- retry topics
- dead-letter topics
- иногда backoff orchestration в consumer/app layer

На собесе сильный ответ такой:

> В Kafka retry обычно строят через отдельные retry/DLQ topics, а не через бесконечные повторы в основном consumer loop. В RabbitMQ retry часто строят через DLX, TTL и промежуточные очереди.

---

## 11. Что сказать про ordering?

### RabbitMQ
Порядок есть, но при конкурирующих consumers и requeue он уже не выглядит как строгая глобальная гарантия для бизнес-сценария.

### Kafka
Порядок гарантируется внутри partition, но не между разными partitions.

---

## 12. Частые practical вопросы

### Как добиться atleast-once delivery?
- RabbitMQ: durable queue + persistent messages + manual ack
- Kafka: careful offset commit after processing

### Как добиться масштабирования?
- RabbitMQ: больше consumers на очереди
- Kafka: больше partitions и consumers в группе

### Как дать нескольким системам читать одно и то же событие?
- RabbitMQ: fanout/topic + отдельные очереди
- Kafka: разные consumer groups

---

## 13. Сильный короткий ответ для собеса

> Если мне нужна классическая очередь задач с удобным routing и понятной operational delivery model, я бы смотрел в RabbitMQ. Если мне нужен event streaming, replay, независимые consumer groups и высокая throughput-модель на append-only log, я бы смотрел в Kafka.
