# Study roadmap: Kafka и RabbitMQ для собеседования

Этот документ нужен, чтобы проходить lab не хаотично, а по шагам.

---

## Общая стратегия подготовки

Тебе не нужно пытаться сразу запомнить весь Kafka/RabbitMQ landscape.

Гораздо эффективнее идти в таком порядке:
1. понять базовую mental model
2. посмотреть working example
3. проговорить это вслух своими словами
4. понять trade-offs
5. закрепить через interview questions

Цель не в том, чтобы выучить документацию наизусть.
Цель в том, чтобы на собеседовании звучать как человек, который:
- понимает архитектурную разницу
- видел это в коде
- знает operational consequences
- может объяснить, почему выбрал бы один инструмент, а не другой

---

# Этап 1. RabbitMQ basics

## Что изучить
Сначала пройди:
- `src/RabbitMq.Producer`
- `src/RabbitMq.Consumer.WorkQueue`
- `src/RabbitMq.Consumer.PubSub`
- `src/RabbitMq.Consumer.DirectRouting`
- `src/RabbitMq.Consumer.TopicErrors`

## Что понять

### Work Queue
Пойми:
- producer пишет в queue
- несколько workers конкурируют за сообщения
- одно сообщение обычно обрабатывает один worker
- manual ack нужен, чтобы не потерять задачу
- `prefetch = 1` помогает fair dispatch

### Fanout Pub/Sub
Пойми:
- exchange рассылает копию во все связанные очереди
- это уже broadcast, а не competing consumers

### Direct Exchange
Пойми:
- routing строится по точному совпадению ключа
- удобно для конечного набора категорий вроде `billing`, `shipping`, `email`

### Topic Exchange
Пойми:
- routing key определяет, кто получит сообщение
- `*` и `#` дают гибкий pattern matching
- это более гибкий вариант по сравнению с `direct`, когда нужны не точные ключи, а шаблоны

## Что уметь сказать вслух

> RabbitMQ хорошо подходит для task queues и routing-driven integration. У него очень явная operational model: producer, exchange, queue, consumer, ack.

## Минимальные вопросы к себе
- зачем нужен `ack`?
- зачем нужен `prefetch`?
- чем `fanout` отличается от `topic`?
- чем work queue отличается от pub/sub?

---

# Этап 2. Kafka basics

## Что изучить
Пройди:
- `src/Kafka.Producer`
- `src/Kafka.Consumer.GroupA`
- `src/Kafka.Consumer.GroupB`
- `src/Kafka.Consumer.RetryDemo`

## Что понять

### Producer
Пойми:
- producer пишет в topic
- у сообщения есть key
- key влияет на partitioning

### Consumer Groups
Пойми:
- одна group читает логически один поток обработки
- другая group может независимо читать те же события
- Kafka масштабирует чтение через partitions

### Retry / DLQ
Пойми:
- проблемные сообщения не должны бесконечно ломать основной consumer loop
- обычно используют retry topics и dead-letter topics

## Что уметь сказать вслух

> Kafka это distributed append-only log. Сообщения читаются по offsets, могут жить по retention policy и переиспользоваться разными consumer groups независимо друг от друга.

## Минимальные вопросы к себе
- что такое offset?
- что такое partition?
- зачем message key?
- чем consumer group отличается от RabbitMQ queue?
- почему retry/DLQ в Kafka обычно строят через topics?

---

# Этап 3. Сравнение RabbitMQ и Kafka

На этом этапе открой:
- `docs/interview-cheat-sheet.md`

## Что нужно уметь объяснить

### RabbitMQ
Выбираю, когда:
- нужна очередь задач
- нужен routing
- важно быстро и понятно доставить задачу worker'у
- operational broker удобнее stream platform

### Kafka
Выбираю, когда:
- нужен event streaming
- нужны разные consumer groups
- нужен replay
- нужен throughput и retention

## Ключевая мысль
Не надо отвечать в стиле:
- RabbitMQ плохой, Kafka хороший
- Kafka современный, RabbitMQ старый

Правильнее так:

> Это разные инструменты под разные модели нагрузки и интеграции.

---

# Этап 4. HTTP -> Broker flow

## Что изучить
Пройди:
- `src/Messaging.Api`

## Что понять

Это уже сценарий ближе к реальной backend-разработке:
- клиент делает HTTP запрос
- API валидирует данные
- API создаёт бизнес-событие
- API публикует это событие в messaging layer

## Что уметь сказать вслух

> API принимает команду, а потом превращает её в event, который дальше могут независимо читать разные подсистемы.

## Вопросы к себе
- где рождается событие?
- чем команда отличается от события?
- зачем один и тот же event публиковать в несколько downstream channels?

---

# Этап 5. Transactional Outbox

## Что изучить
Пройди:
- `src/OutboxDemo.Api`
- `src/OutboxDemo.Worker`

## Что понять

Это одна из самых interview-важных тем.

### Проблема
Если сделать так:
1. сохранить заказ в БД
2. потом отправить event

то при падении между шагами получится рассинхрон.

### Решение
- записать заказ и outbox запись в одной транзакции
- отдельный worker публикует outbox записи

## Что уметь сказать вслух

> Transactional outbox нужен, чтобы не потерять событие между локальным commit в БД и публикацией в broker. Он не делает систему magically exactly-once, но сильно улучшает надёжность и обычно работает в модели at-least-once.

## Вопросы к себе
- какую проблему решает outbox?
- почему это не exactly-once?
- почему consumer должен быть idempotent?
- зачем отдельный worker, а не publish прямо в HTTP handler?

---

# Этап 6. Как реально учить перед собеседованием

## День 1
- RabbitMQ basics
- Kafka basics
- сравнение инструментов

## День 2
- HTTP -> broker flow
- outbox pattern
- retry / DLQ

## День 3
- проговорить вслух ответы на типовые вопросы
- открыть cheat sheet
- руками ещё раз пробежать проекты

---

# Как отвечать сильнее

## Плохой ответ
> Kafka это брокер сообщений, RabbitMQ тоже брокер сообщений.

Слишком плоско и без смысла.

## Лучше
> RabbitMQ это классический message broker с очередями и routing, очень удобный для task distribution и integration. Kafka это distributed log для event streams, где ключевыми становятся partitions, offsets, consumer groups и retention.

---

# Что особенно любят спрашивать

1. Когда RabbitMQ лучше Kafka?
2. Когда Kafka лучше RabbitMQ?
3. Что такое consumer group?
4. Что такое offset?
5. Что такое ack?
6. Как делать retry?
7. Что такое DLQ?
8. Что такое outbox pattern?
9. Почему не публиковать событие прямо после save в DB?
10. Что такое idempotency?

---

# Режим прохождения lab

Идеальный режим такой:

1. читаешь README секцию
2. запускаешь проект
3. смотришь вывод
4. сам объясняешь, что произошло
5. потом сверяешься с cheat sheet

Если можешь объяснить пример без подглядывания, значит материал уже садится в голову.

---

# Финальная цель

После прохождения этого roadmap ты должен уметь уверенно:
- сравнить Kafka и RabbitMQ
- объяснить consumer groups, ack, offset, partition, routing
- рассказать про retry / DLQ
- рассказать про outbox pattern
- показать в коде, как HTTP API публикует события
- объяснить trade-offs, а не просто пересказать термины
