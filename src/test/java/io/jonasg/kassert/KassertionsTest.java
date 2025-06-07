package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KassertionsTest implements KafkaContainerSupport {

    static KafkaConsumer<String, String> consumer;

    static KafkaProducer<String, String> producer;

    @BeforeAll
    static void setup() {
        consumer = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", container.getBootstrapServers(),
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "auto.offset.reset", "earliest",
                "group.id", "test-group"));

        producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", container.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
    }

    @AfterAll
    static void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void rethrowCheckedExceptionAsRuntimeException() {
        assertThatThrownBy(() -> {
            Kassertions.consume("check-exception-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> {
                        throw new IOException("Test exception");
                    });
        }).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Test exception")
                .hasCauseInstanceOf(IOException.class);
    }

    @Nested
    class Contains {

        @Test
        void assertsTopicContains() throws ExecutionException, InterruptedException, TimeoutException {
            var key = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("contains-topic", key, "value"))
                    .get(5, TimeUnit.SECONDS);

            Kassertions.consume("contains-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.contains(key, "value"));
        }

        @Test
        void failWhenTopicDoesNotContain() throws ExecutionException, InterruptedException, TimeoutException {
            var key = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("never-contains-topic", key, "non-matching-value"))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> Kassertions.consume("never-contains-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.contains(key, "value")))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
                            + "'never-contains-topic'. Failed assertions: Expected topic "
                            + "to contain key '%s' with value 'value', "
                            + "but was not found.", key));
        }
    }

    @Nested
    class ContainsKey {

        @Test
        void assertsTopicContainsKey() throws ExecutionException, InterruptedException, TimeoutException {
            var key = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("contains-key-topic", key, "value"))
                    .get(5, TimeUnit.SECONDS);

            Kassertions.consume("contains-key-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsKey(key));
        }

        @Test
        void failWhenTopicDoesNotContainKey() throws ExecutionException, InterruptedException, TimeoutException {
            var key = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("never-contains-key-topic", key, "non-matching-value"))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> Kassertions.consume("never-contains-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsKey(key)))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
                            + "'never-contains-topic'. Failed assertions: Expected topic to contain "
                            + "key '%s', but was not found.", key));
        }
    }

    @Nested
    class ContainsValue {

        @Test
        void assertsTopicContainsValue() throws ExecutionException, InterruptedException, TimeoutException {
            var value = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("contains-value-topic", "key", value))
                    .get(5, TimeUnit.SECONDS);

            Kassertions.consume("contains-value-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsValue(value));
        }

        @Test
        void failWhenTopicDoesNotContainValue() throws ExecutionException, InterruptedException, TimeoutException {
            var value = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("never-contains-value-topic", value, "non-matching-value"))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> Kassertions.consume("never-contains-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsValue(value)))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
                            + "'never-contains-topic'. Failed assertions: Expected topic to contain "
                            + "value '%s', but was not found.", value));
        }
    }

    @Nested
    class ContainsInAnyOrder {

        @Test
        void assertsTopicContainsKeysInAnyOrder() throws ExecutionException, InterruptedException, TimeoutException {
            var key1 = UUID.randomUUID().toString();
            var key2 = UUID.randomUUID().toString();
            var key3 = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("contains-keys-in-any-order", key1, "value"))
                    .get(5, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>("contains-keys-in-any-order", key2, "value"))
                    .get(5, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>("contains-keys-in-any-order", key3, "value"))
                    .get(5, TimeUnit.SECONDS);

            Kassertions.consume("contains-keys-in-any-order", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsKeysInAnyOrder(List.of(key1, key3, key2)));
        }

        @Test
        void failWhenTopicDoesNotContainKeysInAnyOrder()
                throws ExecutionException, InterruptedException, TimeoutException {
            var key1 = UUID.randomUUID().toString();
            var key2 = UUID.randomUUID().toString();
            var topic = "contains-values-in-any-order-" + key1;
            producer.send(new ProducerRecord<>(topic, key1, "value"))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsKeysInAnyOrder(List.of(key1, key2))))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic " +
                            "'%1$s'. Failed assertions: Expected topic to " +
                            "contain keys [%2$s, %3$s] in any order, but [%3$s] could not be found in received keys [%2$s]",
                            topic, key1, key2));
        }

        @Test
        void assertsTopicContainsValuesInAnyOrder() throws ExecutionException, InterruptedException, TimeoutException {
            var value1 = UUID.randomUUID().toString();
            var value2 = UUID.randomUUID().toString();
            var value3 = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>("contains-values-in-any-order", "key", value1))
                    .get(5, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>("contains-values-in-any-order", "key", value2))
                    .get(5, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>("contains-values-in-any-order", "key", value3))
                    .get(5, TimeUnit.SECONDS);

            Kassertions.consume("contains-values-in-any-order", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsValuesInAnyOrder(List.of(value1, value3, value2)));
        }

        @Test
        void failWhenTopicDoesNotContainValuesInAnyOrder()
                throws ExecutionException, InterruptedException, TimeoutException {
            var value1 = UUID.randomUUID().toString();
            var value2 = UUID.randomUUID().toString();
            var topic = "contains-values-in-any-order-" + value1;
            producer.send(new ProducerRecord<>(topic, "key", value1))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(t -> t.containsValuesInAnyOrder(List.of(value1, value2))))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic " +
                            "'%1$s'. Failed assertions: Expected topic to " +
                            "contain values [%2$s, %3$s] in any order, but [%3$s] could not be found in received values [%2$s]",
                            topic, value1, value2));
        }
    }

    @Nested
    class IsEmpty {

        @Test
        void assertsTopicIsEmpty() {
            Kassertions.consume("empty-topic", consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(TopicAssertions::isEmpty);
        }

        @Test
        void failWhenTopicIsNotEmpty() {
            var topic = "non-empty-topic-" + UUID.randomUUID();
            IntStream.range(0, 5)
                    .forEach(i -> {
                        // Simulate some delay to ensure records are sent after the consumer starts
                        // polling
                        new Thread(() -> {
                            try {
                                Thread.sleep(2000);
                                producer.send(new ProducerRecord<>(topic, "key", "value"))
                                        .get(5, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                throw new RuntimeException(e);
                            }

                        }).start();
                    });

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(5))
                    .untilAsserted(TopicAssertions::isEmpty))
                    .hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: "
                            + "Expected topic to be empty, but found 5 records.", topic));
        }

    }

    @Nested
    class HasSize {

        @Test
        void assertsTopicHasExactSize() {
            var key = UUID.randomUUID().toString();
            var topic = "has-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> {
                        new Thread(() -> {
                            try {
                                Thread.sleep(i * 200L);
                                producer.send(new ProducerRecord<>(topic, key, "value"))
                                        .get(5, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                    });

            Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSize(10));
        }

        @Test
        void failWhenTopicHasLessThanExpectedSize() {
            var key = UUID.randomUUID().toString();
            var topic = "does-not-has-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> new Thread(() -> {
                        try {
                            Thread.sleep(i * 200L);
                            producer.send(new ProducerRecord<>(topic, key, "value"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }).start());

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSize(20)))
                    .hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: Expected topic "
                            + "to contain 20 records, but found 10.", topic));
        }

        @Test
        void failWhenTopicHasMoreThanExpectedSize() {
            var key = UUID.randomUUID().toString();
            var topic = "does-not-has-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> new Thread(() -> {
                        try {
                            Thread.sleep(i * 200L);
                            producer.send(new ProducerRecord<>(topic, key, "value"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }).start());

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSize(5)))
                    .hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: Expected topic "
                            + "to contain 5 records, but found 10.", topic));
        }
    }

    @Nested
    class HasSizeGreaterThan {

        @Test
        void assertsTopicHasSizeGreaterThan() {
            var key = UUID.randomUUID().toString();
            var topic = "has-size-greater-then-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> {
                        new Thread(() -> {
                            try {
                                Thread.sleep(i * 200L);
                                producer.send(new ProducerRecord<>(topic, key, "value"))
                                        .get(5, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                    });

            Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSizeGreaterThan(9));
        }

        @Test
        void failWhenTopicHasLessThanMinExpectedSize() {
            var key = UUID.randomUUID().toString();
            var topic = "does-not-has-min-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> new Thread(() -> {
                        try {
                            Thread.sleep(i * 200L);
                            producer.send(new ProducerRecord<>(topic, key, "value"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }).start());

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSizeGreaterThan(20)))
                    .hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: Expected topic to contain "
                            + "more than 20 records, but only found 10 after 10000 ms.", topic));
        }

        @Test
        void failWhenTopicHasEqualNumberOfRecordsAsParam() {
            var key = UUID.randomUUID().toString();
            var topic = "equal-numer-as-param-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> new Thread(() -> {
                        try {
                            Thread.sleep(i * 200L);
                            producer.send(new ProducerRecord<>(topic, key, "value"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }).start());

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSizeGreaterThan(10)))
                    .hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: Expected topic to contain "
                            + "more than 10 records, but only found 10 after 10000 ms.", topic));
        }
    }

    @Nested
    class HasSizeLessThan {

        @Test
        void assertsTopicHasSizeLessThan() {
            var key = UUID.randomUUID().toString();
            var topic = "has-size-greater-then-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> {
                        new Thread(() -> {
                            try {
                                Thread.sleep(i * 200L);
                                producer.send(new ProducerRecord<>(topic, key, "value"))
                                        .get(5, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                    });

            Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSizeLessThan(11));
        }

        @Test
        void failWhenTopicHasLessThanMinExpectedSize() {
            var key = UUID.randomUUID().toString();
            var topic = "does-not-has-mas-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i -> new Thread(() -> {
                        try {
                            Thread.sleep(i * 200L);
                            producer.send(new ProducerRecord<>(topic, key, "value"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }).start());

            assertThatThrownBy(() -> Kassertions.consume(topic, consumer)
                    .within(Duration.ofSeconds(10))
                    .untilAsserted(t -> t.hasSizeLessThan(9)))
                    .hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                            + "'%s'. Failed assertions: Expected topic to contain less than "
                            + "9 records, but found 9 after 10000 ms.", topic));
        }
    }
}