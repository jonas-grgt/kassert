# kassert

Kafka assertion library. Provides fluent assertions over the records received by a `KafkaConsumer`, polling until the assertion is decided or the deadline is reached.

## Example

All you need it a `org.apache.kafka.clients.consumer.Consumer` to get started.

```java
Kassertions.consume(consumer)
    .assignedTo("orders")                             // assign a topic
    .fromBeginning()                                  // optional: seek to the beginning
    .within(5, TimeUnit.SECONDS)                      // optional: deadline (default 5s)
    .filter(rec -> rec.key().equals("k1"))            // optional, chainable (logical AND)
    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
```

The consumer is polled until the deadline, accumulating every record received across all polls. 
The terminal assertions re-run against the accumulated (and filtered) records on each poll and 
return as soon as the outcome is decided; on timeout the failure reports the observed records.

## Installation

```xml
```xml
<dependencies>
    <dependency>
        <groupId>io.jonasg</groupId>
        <artifactId>kassert</artifactId>
        <version>${kassert.version}</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## The assertion steps

### 1. Assign

`Kassertions.consume(consumer)` returns an assignment step:

| Method                                                       | Description                                           |
|--------------------------------------------------------------|-------------------------------------------------------|
| `assignedTo(String topic)`                                   | Assigns all partitions of the given topic.            |
| `assignedTo(String topic, int partition)`                    | Assigns a single partition of the given topic.        |
| `assignedTo(Collection<TopicPartition> partitions)`          | Assigns the given partitions.                         |
| `usingCurrentAssignment()`                                   | Uses the consumer's existing assignment.              |

### 2. Position & deadline

| Method                                   | Description                                                                |
|------------------------------------------|----------------------------------------------------------------------------|
| `fromBeginning()`                        | Seeks all assigned partitions to the beginning before polling.             |
| `fromLast(int n)`                        | Seeks every assigned partition `n` records back before its end.            |
| `within(long timeout, TimeUnit timeUnit)`| Sets the deadline within which the assertion must succeed (default 5s).    |

- `fromBeginning()` and `fromLast(n)` are mutually exclusive; the last call wins.
- Both require the consumer to be assigned and throw `IllegalStateException` otherwise.
- `fromLast(n)` seeks each partition `n` records back, clamped to the beginning of the log.
- When neither is called, the consumer uses its default offset behavior: the group's committed offsets, else `auto.offset.reset` (default `latest`).

### 3. Filter

| Method                                                    | Description                                                  |
|-----------------------------------------------------------|--------------------------------------------------------------|
| `filter(Predicate<ConsumerRecord<K, V>> predicate)`       | Restricts the assertion to matching records. Chainable; multiple filters combine with a logical AND. |

### 4. Terminal assertion

| Method                                                      | Description                                                                                                    |
|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `anySatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | Passes as soon as at least one filtered record satisfies the assertion; fails when the deadline is reached.    |
| `allSatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | Passes once every filtered record satisfies; fails at the deadline or if no records matched the filter.        |
| `noneSatisfy(Consumer<ConsumerRecord<K, V>> assertion)`     | Passes once the deadline is reached with no filtered record satisfying; fails as soon as one does.             |
