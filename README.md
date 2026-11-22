# Kafka Demo

A Spring Boot producer and consumer for Apache Kafka in one Maven build. It replaces the separate
`kafkaproducer` and `kafkaconsumer` repositories.

| Module | Port | |
|---|---|---|
| `contract` | | the `MessageEvent` record both sides share |
| `producer` | 8787 | `POST /message` (text body) and `POST /messages` (`{"text": "..."}`) publish a JSON event keyed by its id |
| `consumer` | 8788 | listens to the topic; `GET /messages` and `GET /messages/dead-letters` show what arrived |

Records that cannot be handled go to the dead letter topic `demo.messages-dlt`: broken JSON or an empty text
right away, other failures after two more attempts. One bad record never blocks the partition.

## Run

Everything in Docker:

```bash
docker compose up --build
curl -X POST -H 'Content-Type: text/plain' --data 'Merhaba Kafka' localhost:8787/message
curl localhost:8788/messages
```

Kafka UI is on http://localhost:8085.

Only Kafka in Docker, the applications from the IDE or Maven:

```bash
docker compose up kafka
./mvnw -pl producer -am spring-boot:run
./mvnw -pl consumer -am spring-boot:run
```

Send a broken record to see the dead letter handling:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo.messages
> this is not json
curl localhost:8788/messages/dead-letters
```

| Setting | Default | |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `app.kafka.topic` | `demo.messages` | both applications |
| `app.kafka.retries`, `app.kafka.retry-backoff` | `2`, `500ms` | consumer |

## Tests

```bash
./mvnw verify    # unit tests, plus *IT tests against Kafka 4.4 in Docker (skipped without Docker)
```

## License

Copyright 2024 Mehmet ERGULCU. Licensed under the [Apache License 2.0](LICENSE).
