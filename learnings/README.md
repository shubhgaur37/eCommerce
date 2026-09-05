# E-commerce Project Learnings

This journal documents only commits authored by
`shubhgaur37 <shubhgaur7833@gmail.com>`. The entries follow commit order
without using calendar dates. Each section explains the implementation change,
the lesson behind it, relevant code/configuration examples, and production
trade-offs. Commits from other authors are intentionally outside this
journal's scope.

## Phase 1 — Hardening configuration and adding ELK

### Completing the configuration move (`03995f3`, `27b4cfd`)

Remaining application settings were moved toward Config Server ownership, and
the Config Server's Git URI, branch, credentials, and port were corrected for
the current external repository.

Key learnings:

- Bootstrap settings and environment settings have different lifecycles.
  Keeping only the former locally makes ownership easier to understand.
- Branch labels matter: a server pointed at `master` will not find a repository
  whose active configuration is on `main`.
- Configuration should be tested directly with
  `/{application}/{profile}` before diagnosing every downstream client.

Historical client bootstrap, still present in
`inventory-service/src/main/resources/application.properties`:

```properties
spring.application.name=inventory-service
spring.config.import=configserver:http://localhost:8888
```

Current Config Server Git backend in
`config-server/src/main/resources/application.yml`:

```yaml
server.port: 8888
spring:
  cloud:
    config:
      server:
        git:
          uri: https://github.com/shubhgaur37/ecommerce-config-server
          password: ${GIT_REPO_TOKEN}
          default-label: main
```

The resulting local service addresses are split between bootstrap configuration
in this repository and environment configuration in the external repository:

| Component | Port | Source of configuration |
|---|---:|---|
| API Gateway | `8080` | Spring Boot default; no `server.port` override is currently configured |
| Config Server | `8888` | Local `config-server/application.yml` |
| Eureka | `8761` | Local discovery-service properties |
| Inventory Service | `9010` | External `inventory-service.properties` |
| Order Service | `9020` | External `order-service.properties` |

Inventory uses the `/inventory` context path and order uses `/orders`. This is
why gateway path rewriting and downstream controller paths must be evaluated
together rather than looking only at controller annotations.

### Refresh-scope placement (`f4df81e`)

Refresh behavior was moved/refined around the feature configuration rather
than indiscriminately annotating consumers of that configuration.

Key learning: scope the mutable lifecycle to the smallest bean that owns
refreshable state. Broad refresh scope makes object recreation and debugging
harder to reason about.

Current reference: `order-service/src/main/java/com/codingshuttle/ecommerce/order_service/config/FeaturesEnableConfig.java`.

```java
@Configuration
@RefreshScope
public class FeaturesEnableConfig {
    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenOrderFlowEnabled;
}
```

### Config Server removed from Eureka (`8ba9f75`)

The Config Server's Eureka-client dependency was removed. Clients already used
the fixed bootstrap URL `http://localhost:8888`, so registration added no value
to the current topology.

Key learnings:

- Discovery-first Config Server lookup is useful when its address changes or it
  has multiple instances, but fixed-URL bootstrap is simpler locally.
- Infrastructure should not advertise capabilities the system does not use.
- Removing a client dependency also removes its transitive behavior and avoids
  confusing registration failures during startup.

The change was a dependency-level correction in `config-server/pom.xml`: the
Config Server retains `spring-cloud-config-server` but no longer declares
`spring-cloud-starter-netflix-eureka-client`.

### Logstash pipeline and secure Elasticsearch connection (`30194b6`, `d34318d`)

`elk-config/logstash.conf` was created to tail service log files and emit
daily `ecommerce-spring-boot-logs-*` indexes. It was then adapted for
Elasticsearch's HTTPS endpoint and the local self-signed certificate.

Key learnings:

- A Logstash pipeline has input, optional filter, and output stages. File
  inputs maintain read position, so an unchanged file may not be reprocessed
  merely because Logstash restarts.
- Container paths must match bind-mounted paths, not host paths.
- `elasticsearch` resolves through Compose DNS inside the network; `localhost`
  inside Logstash refers to the Logstash container itself.
- Disabling certificate verification is a local-development concession. A
  production pipeline should trust and verify the correct certificate chain.

At this stage, the pipeline in `elk-config/logstash.conf` used:

```conf
input {
  file {
    path => "/logs/*/application-*.log"
  }
}

output {
  elasticsearch {
    hosts => ["https://elasticsearch:9200"]
    index => "ecommerce-spring-boot-logs-%{+YYYY.MM.dd}"
    ssl_verification_mode => "none"
  }
}
```

This HTTPS/self-signed configuration records the behavior at these commits. A
later production-preparation commit moved the file to
`infrastructure_config/logstash.conf`, kept Elasticsearch authentication
enabled, and deliberately changed its internal HTTP endpoint; that current
state is documented in Phase 4.

### Compose environment and ELK orchestration (`075965f`, `249324f`, `89155c4`)

Environment variables and `docker-compose.yml` were added for Elasticsearch,
a one-time setup container, Logstash, and Kibana. Elasticsearch data uses a
named volume; Logstash receives its pipeline and application logs through
read-only bind mounts. The setup job waits for Elasticsearch and configures the
`kibana_system` password before Kibana starts.

Key learnings:

- Compose substitutes `${NAME}` from the shell first and then its `.env` file;
  this does not automatically inject those values into Java processes launched
  by an IDE or terminal.
- `depends_on` controls ordering, not readiness, unless paired with a health or
  completion condition. The setup script therefore performs its own readiness
  probe.
- One-shot initialization belongs in a job that can complete successfully,
  while long-running services should remain independently restartable.
- Named volumes preserve managed data across container replacement. Bind
  mounts expose host-owned files and should be read-only when mutation is not
  required.
- Kibana connects with `kibana_system`; the `elastic` superuser is reserved for
  administrative work.

The one-shot dependency is expressed in `docker-compose.yml`:

```yaml
kibana:
  depends_on:
    setup:
      condition: service_completed_successfully

setup:
  depends_on:
    elasticsearch:
      condition: service_started
```

The setup command adds its own `curl` loop because `service_started` alone does
not mean Elasticsearch is ready to accept authenticated API calls.

At this stage, Elasticsearch was exposed to the host over HTTPS with a
self-signed certificate. A direct local check therefore used:

```bash
curl -k -u "elastic:$ELASTIC_PASSWORD" https://localhost:9200
```

The `-k` flag disables certificate verification and should not become a
production default. The later ELK update switched the current Compose endpoint
to authenticated HTTP, so the present-day check no longer uses `-k` or
`https://`.

The ELK stack was started from the repository root with:

```bash
docker compose up -d
```

### Passing the Git token safely (`d4c3a61`, `f9df95a`)

The Config Server changed from an embedded credential to
`${GIT_REPO_TOKEN}`, and the repository documentation was created alongside
history cleanup intended to remove the exposed token.

Key learnings:

- Replacing a secret in the current file is not enough: earlier commits,
  reflogs, clones, caches, and forks may retain it.
- The first response to exposure is revocation or rotation. History rewriting
  reduces disclosure but cannot guarantee deletion from copies already made.
- Secret scanning belongs before push and in CI.
- `.env` is a local convenience, not a production secret manager, and it must
  remain untracked.

Safe indirection in `config-server/src/main/resources/application.yml`:

```yaml
password: ${GIT_REPO_TOKEN}
```

Launch examples:

```bash
export GIT_REPO_TOKEN='replace-with-a-read-only-token'
cd config-server
./mvnw spring-boot:run
```

The placeholder may be committed; the real value must not be.

## Phase 2 — Kafka and the event-driven flow

### Producer configuration deep dive (`200007f`)

Spring Kafka was added to inventory with a host-facing bootstrap address and
producer properties. This established the distinction between a bootstrap
server, topic, record key, serializer, and message value.

Key learnings:

- Bootstrap servers are an entry point for metadata discovery, not necessarily
  the only broker a client will use.
- Kafka returns its advertised listener addresses to clients. Those addresses
  must be resolvable from the client's network.
- Host-run Spring applications use `localhost:29092`; containers on the Docker
  network use `broker:9092`.
- Producer key and value serializers must match the Java types sent through
  `KafkaTemplate`.

Historical configuration introduced by this phase in
`inventory-service/src/main/resources/application.properties`:

```properties
spring.kafka.bootstrap-servers=localhost:29092
```

At this stage the commit established only the broker connection; topic
externalization arrived in `1b1df49`, and explicit serializer configuration
arrived in `c371334`. The business event ultimately changed to a `Long` key and
an Avro value. This snippet represents the initial Kafka learning path, not the
current `OrderConfirmedEvent` settings.

### First success message (`45c409f`)

After inventory was successfully reduced, `ProductService` published a simple
Kafka message. This connected a database-side business action to asynchronous
notification.

Key learnings:

- `KafkaTemplate.send()` is asynchronous. Calling it does not itself prove the
  broker acknowledged the record; completion should be observed when delivery
  matters.
- A Spring database transaction and Kafka publish are not automatically one
  atomic operation. Stock can commit while publishing fails.
- The transactional outbox pattern addresses that gap by committing the
  business change and an outbox row together, then publishing reliably.

Historical publishing pattern from `ProductService`:

```java
kafkaTemplate.send(
    orderCreatedItemsTopicName,
    "Order Created for items: " + productNames
);
```

To observe asynchronous failure instead of ignoring it, a producer can handle
the returned future:

```java
kafkaTemplate.send(topic, message)
    .whenComplete((result, error) -> {
        if (error != null) {
            log.error("Kafka publish failed", error);
        }
    });
```

This improves visibility but still does not make the database and Kafka write
atomic.

### Explicit topic beans (`1b1df49`)

Inventory gained a Kafka configuration class declaring the topic and moved the
topic name into configuration.

Key learnings:

- Explicit topic creation documents partitions and replication instead of
  relying on broker auto-creation defaults.
- A replication factor of one is suitable only for the single-broker local
  environment.
- Externalizing a topic name helps environments differ, but producer and
  consumer property names and resolved values must still agree.
- Partition count sets the upper bound on useful parallelism for one consumer
  group at a point in time.

Current reference:
`inventory-service/src/main/java/com/codingshuttle/ecommerce/inventory_service/config/KafkaConfig.java`.

```java
@Bean
public NewTopic orderCreatedItemTopic() {
    return new NewTopic(orderCreatedItemsTopicName, 3, (short) 1);
}
```

### Consumers, groups, and offsets (`21eb2c2`)

Order gained Spring Kafka consumers. Separate listeners/groups demonstrated
that different groups each receive their own copy, while members of one group
divide partitions. One listener accepted `ConsumerRecord` to inspect topic,
partition, offset, timestamp, headers, key, and value.

Key learnings:

- Ordering is guaranteed only within a partition, not across an entire topic.
- Offsets track a consumer group's progress per partition.
- `auto-offset-reset=earliest` applies only when the group has no valid committed
  offset. It does not rewind an existing group. For replay, reset offsets or
  use a new group ID.
- Record headers are binary and may be sensitive; diagnostic logging should not
  assume every header is safe UTF-8 text.

Current listener examples in
`order-service/src/main/java/com/codingshuttle/ecommerce/order_service/service/OrdersService.java`:

```java
@KafkaListener(
    topics = "${kafka.topic.OrderCreatedItemsTopic}",
    groupId = "order-service-logger"
)
public void logMessage(String message) {
    log.info("OrderCreated Logger Message: {}", message);
}

@KafkaListener(topics = "${kafka.topic.OrderCreatedItemsTopic}")
public void inspectRecord(ConsumerRecord<String, String> record) {
    log.info("topic={}, partition={}, offset={}, value={}",
        record.topic(), record.partition(), record.offset(), record.value());
}
```

With three partitions, at most three active consumers in the same group can
process this topic concurrently; additional group members remain idle.

### Trace propagation over Kafka (`92bc934`)

Micrometer Observation was enabled on both `KafkaTemplate` and Kafka listeners.
This extended trace context beyond synchronous HTTP into producer and consumer
spans carried through Kafka headers.

Key learnings:

- An asynchronous consumer runs later and possibly elsewhere, so thread-local
  HTTP context cannot bridge the boundary by itself.
- Producer instrumentation injects context into headers; consumer
  instrumentation extracts it and starts related work.
- Both ends must enable compatible observation/propagation for a connected
  trace rather than unrelated spans.

Properties added to both producer and consumer applications:

```properties
spring.kafka.template.observation-enabled=true
spring.kafka.listener.observation-enabled=true
```

For a single service only the applicable side is required, but enabling both
is useful here because services contain both demonstration and business Kafka
roles.

### Documentation consolidation (`06b73a1`) and import cleanup (`f13830f`)

The README began capturing operational lessons, and unused imports were
removed. The cleanup did not change behavior, but reinforced that small hygiene
commits make later diffs and compiler feedback easier to read.

### Typed JSON serialization (`c371334`)

The producer used a `LongSerializer` plus Spring's `JsonSerializer`; the order
consumer used matching deserializers. Spring's JSON type metadata and trusted
package rules became part of the event contract.

Key learnings:

- Topic, key type, value type, serializers, and deserializers must be treated as
  one end-to-end contract.
- Spring JSON may add `__TypeId__` with a fully qualified Java class name. This
  couples consumers to producer package names unless type mappings are used.
- `JsonDeserializer` rejects types outside trusted packages as a safety
  measure. Trust should be narrow; `*` removes that protection.
- Copying the same Java event class into two services can work for a learning
  project but creates drift. A shared versioned contract or schema system is
  cleaner.

Historical JSON configuration before the Avro migration:

```properties
# Inventory producer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.LongSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Order consumer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.LongDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.codingshuttle.ecommerce.*
```

The matching types were reflected in the template:

```java
private final KafkaTemplate<Long, OrderConfirmedEvent> kafkaTemplate;
```

This entire JSON setup was superseded by `8e06d59`.

### Event-flow feature flag (`b5d5713`)

The generic feature toggle was renamed to describe its purpose:
`features.event_driven_order_flow.enabled`. The flag selected between the
existing synchronous flow and the developing event-driven flow.

Key learnings:

- Feature flags should name business behavior, not implementation trivia.
- A flag that changes a distributed protocol is not local state. If order and
  inventory disagree, one may expect an event the other never emits.
- Rollout, rollback, default values, and refresh order should be designed along
  with the flag.

Current decision point in
`order-service/src/main/java/com/codingshuttle/ecommerce/order_service/controller/OrdersController.java`:

```java
if (featuresEnableConfig.isEventDrivenOrderFlowEnabled()) {
    orderService.reserveInventory(orderRequestDto);
    return ResponseEntity.ok(null);
}

return ResponseEntity.ok(orderService.createOrder(orderRequestDto));
```

This also exposes a current API-design limitation: the asynchronous branch
returns `200` with a null body. A production API would usually return `202
Accepted` with an order/correlation identifier that clients can query.

### Second topic (`7b917cb`)

`OrderConfirmedTopic` was explicitly declared alongside the earlier
`OrderCreatedItemsTopic`. The first remained useful as a string/group-learning
example; the second carried the business event that completes an order.

Key learning: topics should represent stable streams of facts. Separating the
demonstration message from the order-confirmation event avoids overloading one
topic with incompatible meanings and schemas.

Current topic declaration:

```java
@Bean
public NewTopic orderConfirmedTopic() {
    return new NewTopic(orderConfirmedTopicName, 3, (short) 1);
}
```

### Refresh-scope mechanics (`24d6b92`)

This commit refined which beans need `@RefreshScope`. The order service keeps
the feature flag in `FeaturesEnableConfig`, while inventory reads the same flag
inside `ProductService`:

```java
// order-service
@Configuration
@RefreshScope
public class FeaturesEnableConfig {
    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenOrderFlowEnabled;
}

// inventory-service
@Service
@RefreshScope
public class ProductService {
    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenFlowEnabled;
}
```

`OrdersController` does not need `@RefreshScope` merely because it injects
`FeaturesEnableConfig`. Spring injects a scoped proxy, and after refresh the
next call through that proxy reaches the recreated configuration bean.

The relevant values are supplied by the external configuration repository:

```properties
my.variable=...
features.event_driven_order_flow.enabled=false
management.endpoints.web.exposure.include=refresh
```

The management property is required for `POST /actuator/refresh` to be
available over HTTP. Exposing sensitive Actuator endpoints should be restricted
appropriately outside local development.

#### Testing configuration refresh

1. Change the relevant service configuration in the external configuration
   repository and push the change.
2. Confirm the Config Server returns the updated property source.
3. Refresh every running service that participates in the changed behavior:

```bash
curl -X POST http://localhost:<order-service-port>/actuator/refresh
curl -X POST http://localhost:<inventory-service-port>/actuator/refresh
```

4. Call `GET /core/helloOrders` and exercise order creation to verify that both
   services now follow the refreshed feature flag.

The port placeholders must be replaced using the external configuration or
the service startup logs.

`@RefreshScope` refresh is lazy: the refresh operation invalidates the scoped
target, and the next use recreates it with the new `@Value` properties. The
endpoint's response may list changed property keys, but the behavior should
still be tested.

There is an important nuance in the current code. `my.variable` is injected
directly into `OrdersController`, which is no longer refresh-scoped:

```java
public class OrdersController {
    @Value("${my.variable}")
    private String myVariable;

    private final FeaturesEnableConfig featuresEnableConfig;
}
```

Therefore the feature flag can refresh through the scoped
`FeaturesEnableConfig` proxy, but the controller's direct `my.variable` value
can remain stale. To make the sample variable refresh reliably, either move it
into `FeaturesEnableConfig` and read it through the proxy, or place the
controller itself back under `@RefreshScope`. Moving all refreshable settings
into the dedicated configuration bean keeps the lifecycle more focused.

Finally, the feature flag changes a distributed protocol: order uses it to
select the request path, while inventory uses it to decide whether to publish
`OrderConfirmedEvent`. Refreshing only one service can leave the system in an
inconsistent state, so both services must be refreshed together.

### Full event-driven order completion (`9157316`)

Inventory now calculated the total, reduced stock, created an
`OrderConfirmedEvent`, and published it when the flag was enabled. Order
consumed that event, mapped items, set bidirectional entity ownership, marked
the order `CONFIRMED`, and persisted it. The same event class was temporarily
duplicated under a matching fully qualified name in both modules to satisfy
Spring JSON type resolution.

Key learnings:

- Events should describe completed facts—inventory has been reserved—rather
  than remotely command the consumer's internal implementation.
- A consumer should be idempotent because Kafka delivery can be repeated. The
  current flow needs an event/order identifier and uniqueness strategy before
  it is production-safe.
- Mapping nested DTO/event objects to JPA entities requires setting the owning
  side of relationships explicitly.
- Convention mapping can confuse `productId` with an entity primary key `id`;
  generated entity IDs must be cleared or explicitly mapped.
- Database update plus publish remains a dual-write risk; database save in the
  consumer also needs retry/dead-letter and poison-message policies.

Historical producer flow, simplified from `ProductService` at `9157316`:

```java
Double totalPrice = reduceStocks(orderRequestDto);
OrderConfirmedEvent event = modelMapper.map(
    orderRequestDto, OrderConfirmedEvent.class
);
event.setTotalPrice(totalPrice);
kafkaTemplateOrderConfirmed.send(orderConfirmedTopicName, event);
```

Historical consumer flow, simplified from `OrdersService` at the same commit:

```java
@KafkaListener(
    topics = "${kafka.topic.OrderConfirmedTopic}",
    groupId = "${kafka.consumer.order_creation.group.id}"
)
public void createOrderFromEvent(OrderConfirmedEvent event) {
    Orders order = modelMapper.map(event, Orders.class);
    order.getItems().forEach(item -> item.setOrder(order));
    order.setOrderStatus(OrderStatus.CONFIRMED);
    orderRepository.save(order);
}
```

These examples show the flow but predate Avro and omit the later fix that clears
incorrectly mapped item IDs.

### README and architecture refinements (`288ec65`, `412f1a3`, `3a46814`, `61f2795`, `f61ec64`)

Several commits reorganized project documentation, corrected architecture
diagrams, documented the ELK index/data-view flow, and clarified that `.env`
loading depends on whether Compose, an IDE, or a terminal launches a process.
These commits captured an important engineering lesson: operational behavior
is part of the system and should be documented alongside code.

Three merge commits (`b094448`, `f689f94`, `aa0244c`) reconciled the local and
remote `main` histories around the README work. They introduced no distinct
runtime capability beyond their merged parents. Their lesson is procedural:
parallel documentation edits can conflict just like code, so resolve them by
checking the resulting document for duplicated, stale, or contradictory
sections rather than trusting a clean textual merge alone.

### Introducing a dedicated learning section (`075a2bc`)

The main README first gained a consolidated learning section. This established
the distinction between describing the current project and preserving the
reasoning behind its incremental implementation. Later documentation commits
completed the physical separation after the Avro work.

## Phase 3 — Migrating event contracts to Avro

### Avro, generated records, and Schema Registry (`8e06d59`)

The JSON `OrderConfirmedEvent` contract was replaced with an Avro schema in
both producer and consumer modules. The schema defines an array of nested
`OrderRequestItem` records and a `double` total price in the shared namespace
`com.codingshuttle.ecommerce.events`. The Avro Maven plugin generates
`SpecificRecord` Java classes; inventory uses `KafkaAvroSerializer`, and order
uses `KafkaAvroDeserializer` with `specific.avro.reader=true`. Both use Schema
Registry at `http://localhost:8081`.

Key learnings:

- Avro moves the contract from a producer Java class name to a schema with a
  namespace, fields, and governed evolution rules.
- Confluent's wire format carries a schema identifier; consumers retrieve the
  corresponding schema from Schema Registry rather than requiring Spring JSON
  type headers.
- `specific.avro.reader=true` is necessary when the consumer wants generated
  `SpecificRecord` instances instead of `GenericRecord`.
- Producer and consumer need compatible schemas. Keeping duplicate `.avsc`
  files works only while they remain identical; a shared contract artifact or
  registry-driven build process would reduce drift.
- The schema namespace controls the generated Java package, so handwritten
  services in other packages need explicit imports.
- Maven must bind `avro:schema` to `generate-sources` before compilation. If the
  plugin is absent from one module, generated event classes will not exist
  there.
- Generated code and the Avro runtime/plugin versions must align. Stale output
  can produce confusing missing-API errors, so `mvn clean` and regeneration are
  important.
- Generated sources are normally cleaner under
  `target/generated-sources/avro` than `src/main/java`; the current POM writes
  them into `src/main/java`, so generated files are committed and can become
  stale.
- Dependency compatibility crosses ecosystems. Confluent 8.3.x expected a
  newer Kafka client API and caused a missing `Monitorable` class with the
  Spring Kafka 3.3.4/Kafka 3.7.x line. Confluent 7.7.11 was selected to align
  with that client generation.
- Avro runtime security behavior can change across patch releases. The project
  pinned Avro 1.12.1 after encountering stricter class allow-list behavior in
  1.12.2; production code should understand and configure the security model
  rather than indefinitely avoiding updates.
- Nested record mapping needed enrichment of item names in inventory. On the
  order side, mapped entity IDs are reset before JPA persistence and the owning
  relationship is established explicitly.

Schema source in
`inventory-service/src/main/resources/avro/order-confirmed-event.avsc` (also
present in the order service):

```json
{
  "type": "record",
  "name": "OrderConfirmedEvent",
  "namespace": "com.codingshuttle.ecommerce.events",
  "fields": [
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderRequestItem",
          "fields": [
            { "name": "productId", "type": "long" },
            { "name": "name", "type": "string" },
            { "name": "quantity", "type": "int" }
          ]
        }
      }
    },
    { "name": "totalPrice", "type": "double" }
  ]
}
```

Maven generation in each service's `pom.xml`:

```xml
<plugin>
  <groupId>org.apache.avro</groupId>
  <artifactId>avro-maven-plugin</artifactId>
  <version>1.12.1</version>
  <executions>
    <execution>
      <phase>generate-sources</phase>
      <goals><goal>schema</goal></goals>
    </execution>
  </executions>
</plugin>
```

Current wire configuration:

```properties
# Inventory producer
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.producer.properties.schema.registry.url=http://localhost:8081

# Order consumer
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.schema.registry.url=http://localhost:8081
spring.kafka.consumer.properties.specific.avro.reader=true
```

The local Kafka Compose environment exposes the broker to host-run Spring
applications at `localhost:29092`, Schema Registry at `localhost:8081`, and
Kafbat at `localhost:8085`. Containers use `broker:9092` instead of the host
listener. Start the environment with:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

The current Kafbat bind mount contains a workstation-specific absolute path,
which must be changed when the repository is run on another machine.

Current mapping correction in `OrdersService`:

ModelMapper caused the ID issue. Its implicit matching treated the source
event's `productId` as a match for the destination entity's `id` because the
names are similar and both values are compatible `Long` types. That copied a
product identifier into `OrderItem.id`, which is a separate JPA-generated
primary key. Setting it back to `null` lets Hibernate generate the correct row
identifier. The `setOrder(orders)` call solves a different problem: it sets the
owning side of the bidirectional JPA relationship.

```java
for (OrderItem item : orders.getItems()) {
    item.setId(null);       // undo ModelMapper's productId -> id mapping
    item.setOrder(orders);  // establish the JPA owning relationship
}
```

A cleaner long-term fix is to configure ModelMapper to skip the entity ID
instead of repairing it after every mapping:

```java
modelMapper.typeMap(OrderRequestItem.class, OrderItem.class)
    .addMappings(mapper -> mapper.skip(OrderItem::setId));
```

MapStruct could also have prevented this accidental detached-entity issue and
would be a stronger fit when mapping event contracts into JPA entities.
MapStruct generates mapping code at compile time and normally requires an
explicit rule when source and destination property names differ. Therefore,
`productId` would not ordinarily be mapped into `id` merely because the names
look similar. The intent can be made unambiguous:

```java
@Mapper(componentModel = "spring")
public interface OrderMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "order", ignore = true)
    OrderItem toEntity(OrderRequestItem item);

    Orders toEntity(OrderConfirmedEvent event);

    @AfterMapping
    default void connectItems(@MappingTarget Orders order) {
        order.getItems().forEach(item -> item.setOrder(order));
    }
}
```

Ignoring `id` leaves the new entity transient so Hibernate can generate its
primary key. The `@AfterMapping` method establishes the owning side of the JPA
relationship. MapStruct does not inherently understand Hibernate entity
states, however: an explicit `productId -> id` mapping could still recreate
the same problem. Its advantage here is deterministic, reviewable,
compile-time-generated mapping instead of ModelMapper's runtime name
heuristics.

## Phase 4 — Containerizing the production-profile deployment

### Adding environment-specific service configuration (`21edcc9`)

Production profile files were added for order and inventory. The important
distinction is network identity: a host-run application reaches dependencies
through `localhost` and published ports, while a container reaches other
Compose services through their service names and container ports.

For example, the inventory production profile overrides Kafka and Schema
Registry without duplicating the common application configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: broker:9092
    producer:
      properties:
        schema.registry.url: http://schema-registry:8081
```

The matching external configuration repository now provides:

- `application-prod.properties` for Docker-network Eureka and Zipkin URLs.
- `inventory-service-prod.properties` for the `product-db` connection.
- `order-service-prod.properties` for the `orders-db` connection.

Spring combines the default and `prod` property sources. Shared values remain
in the default files; only environment-dependent addresses need production
overrides.

### Reusing Compose's default network (`b51db33`)

The standalone ELK definition was moved to `docker-compose.elk.yml`, and its
explicit custom network was removed. Compose automatically creates a project
network and connects services to it unless configured otherwise.

Key learning: an explicit network is useful when isolation, a stable external
network, or custom networking settings are required. For one composed
application, the default network is simpler and still provides DNS resolution
by service name. This also allows services from the root Compose file and its
included Kafka/ELK files to communicate on the same project network.

### Keeping infrastructure configuration together (`b64cfaf`)

The Logstash pipeline moved from `elk-config/` to
`infrastructure_config/logstash.conf`, alongside the Kafbat configuration.
The Compose bind mount was updated to match.

Key learning: bind-mounted configuration is resolved from the host path in the
Compose file. Moving a file without updating the mount causes the container to
start with a missing file, an empty directory mount, or its image defaults.

### Externalizing Config Server Git credentials (`b0ae9a5`)

The Config Server now receives both Git credentials from its environment:

```yaml
username: ${GIT_USERNAME}
password: ${GIT_REPO_TOKEN}
```

This removed machine-specific identity from application configuration. The
values are supplied to the container by the root Compose file and should come
from the deployment environment or an untracked local `.env` file.

### Resolving Config Server correctly in both environments (`abd9cb7`)

Config Data imports are processed early in Spring Boot startup. Defining one
Config Server import in the default file and another in a production profile
caused the application to process an unwanted `localhost:8888` location inside
the container. Inside a container, `localhost` points back to that same
container—not to the Config Server service.

The fix was one import with an environment override and a local fallback:

```properties
spring.config.import=configserver:${CONFIG_SERVER_URL:http://localhost:8888}
```

For Docker, Compose supplies:

```yaml
environment:
  CONFIG_SERVER_URL: http://config-server:8888
```

For a host-run application, the variable can be omitted and the expression
falls back to `http://localhost:8888`. This keeps a single Config Data import
and varies only its address.

### Updating ELK security and resource usage (`eba3475`)

Elasticsearch authentication remains enabled, but HTTP TLS is now explicitly
disabled for this Compose environment. Logstash, Kibana, and the setup job
therefore use `http://elasticsearch:9200` while still authenticating with
credentials.

```yaml
environment:
  - xpack.security.enabled=true
  - xpack.security.http.ssl.enabled=false
  - ES_JAVA_OPTS=-Xms512m -Xmx512m
mem_limit: 1g
```

Current host check:

```bash
curl -u "elastic:$ELASTIC_PASSWORD" http://localhost:9200
```

This is different from the earlier self-signed HTTPS setup. Authentication and
transport encryption are separate controls: enabling security does not require
disabling TLS, and a real production deployment should normally use both
authentication and verified TLS. The heap and container memory limits make
the learning stack less resource-intensive but must be sized from workload
measurements in a real environment.

### Adding application Dockerfiles (`6b51bf7`)

Each Spring application gained a Dockerfile based on Maven and JDK 21. The
build copies Maven metadata first, resolves dependencies in a cacheable layer,
then copies the source:

```dockerfile
FROM maven:3.9.6-eclipse-temurin-21
WORKDIR /app
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline
COPY src ./src
CMD ["./mvnw", "spring-boot:run"]
```

The cache boundary avoids downloading every dependency again when only source
code changes. This is suitable for demonstrating containerized services, but a
production-optimized image would normally use a multi-stage build: compile the
JAR in a Maven builder image, then run it in a smaller JRE-only image rather
than starting through Maven.

### Removing a redundant gateway production file (`50086db`)

The empty/redundant API Gateway production property file was removed after the
Config Server URL became environment-driven in the default bootstrap file.

Key learning: profile files should contain real overrides. Keeping an empty
file suggests environment-specific behavior that does not exist and can hide
where bootstrap values actually come from.

### Defining the Kafka platform in Compose (`2145c19`)

`docker-compose.kafka.yml` now defines a single-node KRaft broker, Schema
Registry, and Kafbat. The broker advertises separate listeners:

```yaml
KAFKA_LISTENERS: DOCKER://:9092,HOST://:29092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: DOCKER://broker:9092,HOST://localhost:29092
```

Host-run applications use `localhost:29092`; containers use `broker:9092`.
Schema Registry is published on `8081`, and Kafbat maps host port `8085` to its
container port `8080`. Its configuration is now committed under
`infrastructure_config/kafbat_config.yml` and mounted through a relative path,
making the Compose file portable across workstations.

### Composing the complete production-profile platform (`e928ee9`)

The root `docker-compose.yml` includes the Kafka and ELK definitions and adds
two PostgreSQL databases, all five Spring services, Zipkin, production log
mounts, and named database volumes.

```yaml
include:
  - docker-compose.kafka.yml
  - docker-compose.elk.yml

services:
  order-service:
    image: order_service:1.0
    environment:
      SPRING_PROFILES_ACTIVE: prod
      CONFIG_SERVER_URL: http://config-server:8888
```

Important operational learnings:

- Compose image names must match the tags built from each service directory.
- Only API Gateway is published for application traffic; Config Server,
  Eureka, Order, and Inventory remain internal to the Compose network.
- Product and order PostgreSQL publish different host ports (`5433` and
  `5434`) while both listen on `5432` inside their containers.
- Application logs are written into `./PROD_LOGS`, and the root Compose service
  overrides Logstash's local log mount to ingest that directory.
- Named volumes preserve both databases across container recreation.
- `depends_on` expresses startup ordering, but most entries do not wait for
  application readiness. Services can take time to compile, boot, fetch remote
  configuration, register with Eureka, and connect to infrastructure.
- `docker compose ps` shows container state, while
  `docker compose logs -f order-service` reveals application-level startup
  progress and failures.

The database credentials in the current Compose and external production
properties are fixed demonstration values. A real production deployment must
inject them through secrets rather than commit them.

## Cross-cutting conclusions and next steps

The history shows a progression from independent CRUD services, through
synchronous distributed calls, toward asynchronous schema-governed messaging
and full-stack observability. The most important next production steps are:

1. Add an event/order identifier and make the consumer idempotent.
2. Replace the database-plus-Kafka dual write with a transactional outbox.
3. Introduce a saga pattern as the workflow expands across more services and
   Kafka topics, including compensating events for partially completed steps.
