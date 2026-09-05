# Event-Driven E-commerce Application

## Introduction

An event-driven Spring Boot e-commerce system built with Spring Cloud, Kafka,
and Docker-based observability. It demonstrates synchronous and asynchronous
order flows, centralized configuration, runtime feature toggles, tracing, and
ELK log aggregation.

Detailed implementation notes and code examples are available in the
[learning journal](learnings/README.md).

## What is in this repository

| Area                      | Implementation                                              | Purpose                                                                                                                    |
|---------------------------|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Service discovery         | `discovery-service` (Eureka, port `8761`)                   | Lets services register and locate one another.                                                                             |
| Centralized configuration | `config-server` (port `8888`)                               | Serves configuration from the separate [configuration repository](https://github.com/shubhgaur37/ecommerce-config-server). |
| Edge service              | `api-gateway`                                               | Spring Cloud Gateway with logging and JWT-related filters.                                                                 |
| Business services         | `inventory-service`, `order-service`                        | Inventory and order APIs, PostgreSQL/JPA support, and OpenFeign clients.                                                   |
| Event messaging           | Apache Kafka                                                | Inventory publishes order-item events; order service consumes them in independent consumer groups.                         |
| Distributed tracing       | Micrometer Observation, Brave, Zipkin reporter dependencies | Propagates trace context across HTTP and Kafka when observation is enabled.                                                |
| Log aggregation           | Elasticsearch, Logstash, Kibana                             | Reads local service log files and makes them searchable in Kibana.                                                         |

## Architecture at a glance

```mermaid
flowchart LR
    configRepo["External configuration repository"] --> configServer["Config Server :8888"]

    subgraph platform["Central platform"]
        direction TB
        gateway["API Gateway"]
        discovery["Eureka Discovery :8761"]
    end

    subgraph applications["Business services"]
        order["Order Service"]
        inventory["Inventory Service"]
    end

    configServer --> gateway
    configServer --> order
    configServer --> inventory

    gateway <--> order
    gateway <--> inventory
    discovery <--> gateway
    discovery <--> order
    discovery <--> inventory

    kafka[(Kafka)]
    inventory -->|OrderConfirmedEvent| kafka
    kafka -->|OrderConfirmedTopic| order
```

## Centralized configuration

`config-server/src/main/resources/application.yml` enables Spring Cloud Config
Server and points it at the
[`ecommerce-config-server`](https://github.com/shubhgaur37/ecommerce-config-server)
Git repository. Each config client identifies itself using
`spring.application.name` and imports:

```properties
spring.config.import=configserver:http://localhost:8888
```

For example, `order-service` requests its `order-service` configuration from
the Config Server. This keeps environment-specific service configuration out
of the application source repositories. A service cannot start normally if it
cannot resolve this required Config Server import.

## Kafka event messaging and tracing

The `features.event_driven_order_flow.enabled` feature flag selects the order
flow. When disabled, the order service synchronously reserves inventory and
persists the order. When enabled, inventory reserves stock and publishes an
Avro `OrderConfirmedEvent`; the order service consumes it and persists the
confirmed order.

```mermaid
flowchart LR
    request["POST /core/create-order"] --> flag{"Event-driven flow enabled?"}
    flag -->|No| sync["Reserve inventory and save order"]
    flag -->|Yes| inventory["Inventory reserves stock"]
    inventory --> event["Publish OrderConfirmedEvent"]
    event --> kafka[(OrderConfirmedTopic)]
    kafka --> order["Order Service saves order"]
```

### Topics and consumer groups

Inventory explicitly declares `OrderCreatedItemsTopic` and
`OrderConfirmedTopic`, each with three partitions and replication factor one.
The first is a string-message demonstration; the second carries the business
event. A replication factor of one is suitable only for the local single-broker
setup.

Different consumer groups each receive their own copy of a record, while
consumers in one group divide the partitions. The configured
`auto-offset-reset=earliest` applies only when a group has no valid committed
offset; it does not rewind an existing group.

### Avro event contract

Both business services contain
`src/main/resources/avro/order-confirmed-event.avsc`. Maven generates the
specific `OrderConfirmedEvent` and `OrderRequestItem` classes from this
schema.

Inventory publishes using `KafkaAvroSerializer`:

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.LongSerializer
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.producer.properties.schema.registry.url=http://localhost:8081
```

Order consumes generated specific records using `KafkaAvroDeserializer`:

```properties
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.LongDeserializer
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081
spring.kafka.consumer.properties.specific.avro.reader=true
```

Producer and consumer schemas must remain compatible. A shared, versioned
contract artifact would reduce the risk of the two schema copies drifting.

### Database and event consistency

`KafkaTemplate.send()` is asynchronous, and the stock update plus Kafka
publish are not one atomic operation. A production implementation should use a
transactional outbox, an idempotent consumer, and retry/dead-letter handling.

### Kafka trace propagation

Both services enable Micrometer Observation for Kafka templates and listeners:

```properties
spring.kafka.template.observation-enabled=true
spring.kafka.listener.observation-enabled=true
```

With the existing Micrometer, Brave, and Zipkin dependencies, trace context is
propagated in Kafka headers across the asynchronous boundary.

## Refresh scope: update configuration without a restart

`FeaturesEnableConfig` and `ProductService` are annotated with `@RefreshScope`.
The controller reads the refresh-scoped configuration bean, so it does not need
its own `@RefreshScope` annotation. The feature flag and sample variable are
externally supplied properties:

```properties
my.variable=...
features.event_driven_order_flow.enabled=false
```

The `/core/helloOrders` endpoint reflects the current values. The same flag
must be refreshed consistently in order and inventory services: order uses it
to choose the request path, while inventory uses it to decide whether to emit
`OrderConfirmedEvent`. To test a refresh:

1. Change and push the relevant service configuration in the
   [external configuration repository](https://github.com/shubhgaur37/ecommerce-config-server).
2. Ensure the affected service exposes the Spring Boot Actuator refresh endpoint
   (typically `management.endpoints.web.exposure.include=refresh`).
3. Trigger the refresh on each affected running service:

   ```bash
   curl -X POST http://localhost:<order-service-port>/actuator/refresh
   curl -X POST http://localhost:<inventory-service-port>/actuator/refresh
   ```

4. Call `GET /core/helloOrders` again. The refreshed `my.variable` and
   feature flag are used without restarting the service.

`@RefreshScope` recreates the scoped bean on refresh, so its injected `@Value`
properties do not remain stale. The service ports are externalized and should
be taken from the configuration repository or service startup logs.

## API Gateway configuration and filters

`api-gateway` is a Config Server client. The committed configuration only sets
its application name and Config Server import; routes, JWT settings, and
environment-specific gateway configuration are expected in the external
configuration repository.

| Component                            | Scope                                  | Current behavior                                                                                                                                                                                 |
|--------------------------------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `GlobalLoggingFilter`                | Every gateway request                  | Logs the request URI before routing and the response status after completion. Its order is `5`.                                                                                                  |
| `LoggingOrdersFilter`                | A route that declares `LoggingOrders`  | Logs the request URI before forwarding.                                                                                                                                                          |
| `AuthenticationGatewayFilterFactory` | A route that declares `Authentication` | When `enabled`, requires an `Authorization: Bearer <JWT>` header, validates it with `jwt.secretKey`, and adds `X-User-Id` to the forwarded request. A missing header returns `401 Unauthorized`. |
| `OrdersService` circuit breaker      | Order-to-inventory Feign call          | The active `inventoryCircuitBreaker` invokes `createOrderFallback` when the inventory call fails.                                                                                                |

Here is a representative gateway configuration to place in the external
`api-gateway` configuration. Filter names are derived from the factory class
names with `GatewayFilterFactory` removed.

```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: orders
          uri: lb://order-service
          predicates:
            - Path=/orders/**
          filters:
            - name: Authentication
              args:
                enabled: true
            - name: LoggingOrders

jwt:
  secretKey: ${JWT_SECRET_KEY}
```

`lb://order-service` resolves instances through service discovery. Store
`JWT_SECRET_KEY` outside this repository and make sure it is long enough for
the JWT signing algorithm in use.

### Circuit breaker

Circuit breaking is implemented in `order-service`, where the synchronous
inventory call is protected by:

```java
@CircuitBreaker(
    name = "inventoryCircuitBreaker",
    fallbackMethod = "createOrderFallback"
)
```

The matching `inventoryCircuitBreaker` settings belong in the external
order-service configuration repository. Retry, service rate limiting, and
gateway rate limiting are not active features in this repository.

## Observability and centralized logging

```mermaid
flowchart LR
    services["Spring Boot services"] --> logs["/logs/*/application-*.log"]
    logs --> logstash["Logstash"]
    logstash --> elasticsearch[("Elasticsearch :9200")]
    elasticsearch --> kibana["Kibana :5601"]
```

The ELK containers communicate over the Compose `elk` network using service
names such as `elasticsearch`; `localhost` is only for access from your host
machine.

To view application logs in Kibana, create a data view that matches the
Elasticsearch index configured in [`elk-config/logstash.conf`](elk-config/logstash.conf):

```text
ecommerce-spring-boot-logs-*
```

Logstash creates daily indexes using the pattern
`ecommerce-spring-boot-logs-%{+YYYY.MM.dd}`. After creating the data view,
open **Discover** in Kibana to search the aggregated service logs.

## Prerequisites

- Docker Desktop with Docker Compose v2
- Java 21 (the projects use Spring Boot `3.3.4`)
- A reachable configuration Git repository and a token that can read it
- PostgreSQL instances/settings supplied through the configuration repository
  for the order and inventory services

Create a local `.env` file in the repository root. It is intentionally not a
place to commit real credentials.

```dotenv
ELASTIC_PASSWORD=choose-a-local-elastic-password
KIBANA_PASSWORD=choose-a-local-kibana-system-password
GIT_REPO_TOKEN=your-read-token-for-the-config-repository
```

`docker-compose.yml` consumes the first two values. The Config Server consumes
`GIT_REPO_TOKEN` when it clones/reads its configured Git repository. The token
must be available to the Config Server process, but how it is supplied depends
on how the service is started:

- **IntelliJ IDEA:** configure the run configuration to load the repository's
  `.env` file; IntelliJ passes its values to the application process.
- **Terminal:** export the variable before starting the Config Server:

```bash
export GIT_REPO_TOKEN='your-read-token-for-the-config-repository'
```

## Credential safety

The local ELK passwords can be kept in `.env` for this learning setup. Never
place a GitHub access token, private key, or production credential in a tracked
file. `GIT_REPO_TOKEN` should be supplied as an environment variable when the
Config Server starts, or injected by a secret manager in a deployed
environment.

If an access token reaches a commit, revoke or rotate it immediately. Removing
it from the latest file does **not** remove it from Git history, cloned
repositories, or remote forks; history cleanup and a forced push may also be
needed. Scan before pushing with a secret-scanning tool such as Gitleaks,
GitHub secret scanning, or your CI provider's equivalent.

## Run the ELK stack with Docker Compose

Start the log platform from the repository root:

```bash
docker compose up -d
docker compose ps
```

Open Kibana at <http://localhost:5601>. Elasticsearch is exposed over HTTPS at
<https://localhost:9200>; it uses a local self-signed certificate, so command
line checks need `-k`.

```bash
curl -k -u "elastic:$ELASTIC_PASSWORD" https://localhost:9200
```

The Compose services have the following responsibilities:

1. `elasticsearch` runs as a single-node local cluster and persists data in
   the named `elastic_search_data` Docker volume.
2. `setup` waits for Elasticsearch, then sets the `kibana_system` password.
   It is expected to exit successfully after this one-time initialization.
3. `kibana` starts only after `setup` finishes successfully.
4. `logstash` mounts `elk-config/logstash.conf` and the local `./logs`
   directory read-only, tails `application-*.log` files, and indexes events as
   `ecommerce-spring-boot-logs-YYYY.MM.dd`.

In Kibana, create a data view matching:

```text
ecommerce-spring-boot-logs-*
```

Then use **Discover** to search the application logs. `docker compose logs -f
logstash` is useful while confirming that files are being read and indexed.

Stop the stack while keeping indexed data:

```bash
docker compose down
```

To intentionally discard the local Elasticsearch data as well:

```bash
docker compose down -v
```

## Run the Spring services

Run each service from its own directory using the Maven wrapper. The order is
important because the order, inventory, and gateway applications import their
configuration from the Config Server at startup.

```bash
cd config-server && ./mvnw spring-boot:run
cd discovery-service && ./mvnw spring-boot:run
cd order-service && ./mvnw spring-boot:run
cd inventory-service && ./mvnw spring-boot:run
cd api-gateway && ./mvnw spring-boot:run
```

Use a separate terminal for each command. Confirm the Config Server can return
an application's configuration before starting clients:

```bash
curl http://localhost:8888/order-service/default
```

The Eureka dashboard is available at <http://localhost:8761>. Application
ports, routes, database connections, Eureka-client settings, and management
endpoint exposure may be supplied by the external configuration repository,
not by the small bootstrap configuration stored here.

## Useful development checks

```bash
# Validate resolved Docker Compose syntax (requires the environment values).
docker compose config -q

# Follow the ELK pipeline.
docker compose logs -f logstash

# Run one service's tests.
cd order-service && ./mvnw test
```

## Repository layout

```text
api-gateway/        Gateway filters and gateway application
config-server/      Git-backed Spring Cloud Config Server
discovery-service/  Eureka Server
elk-config/         Logstash pipeline configuration
learnings/          Chronological engineering journal
inventory-service/  Inventory API and order-service Feign client
logs/               Runtime logs consumed by Logstash (generated locally)
order-service/      Order API, Feign client, and refresh-scope example
docker-compose.yml        Elasticsearch, Logstash, Kibana, and setup container
docker-compose.kafka.yml  Kafka, Schema Registry, and Kafbat UI
```
