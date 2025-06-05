package io.jonasg.kassert;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
                "group.id", "test-group"
        ));

        producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", container.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        ));
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

            assertThatThrownBy(() ->
                    Kassertions.consume("never-contains-topic", consumer)
                            .within(Duration.ofSeconds(5))
                            .untilAsserted(t -> t.contains(key, "value"))
            ).hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
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

            assertThatThrownBy(() ->
                    Kassertions.consume("never-contains-topic", consumer)
                            .within(Duration.ofSeconds(5))
                            .untilAsserted(t -> t.containsKey(key))
            ).hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
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

            assertThatThrownBy(() ->
                    Kassertions.consume("never-contains-topic", consumer)
                            .within(Duration.ofSeconds(5))
                            .untilAsserted(t -> t.containsValue(value))
            ).hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
                                       + "'never-contains-topic'. Failed assertions: Expected topic to contain "
                                       + "value '%s', but was not found.", value));
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
                    .forEach(i ->
                            new Thread(() -> {
                                try {
                                    Thread.sleep(i * 200L);
                                    producer.send(new ProducerRecord<>(topic, key, "value"))
                                            .get(5, TimeUnit.SECONDS);
                                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                    throw new RuntimeException(e);
                                }
                            }).start());

            assertThatThrownBy(() ->
                    Kassertions.consume(topic, consumer)
                            .within(Duration.ofSeconds(10))
                            .untilAsserted(t -> t.hasSize(20))
            ).hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                                       + "'%s'. Failed assertions: Expected topic "
                                       + "to contain 20 records, but found 10.", topic));
        }

        @Test
        void failWhenTopicHasMoreThanExpectedSize() {
            var key = UUID.randomUUID().toString();
            var topic = "does-not-has-size-topic-" + key;
            IntStream.range(0, 10)
                    .forEach(i ->
                            new Thread(() -> {
                                try {
                                    Thread.sleep(i * 200L);
                                    producer.send(new ProducerRecord<>(topic, key, "value"))
                                            .get(5, TimeUnit.SECONDS);
                                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                    throw new RuntimeException(e);
                                }
                            }).start());

            assertThatThrownBy(() ->
                    Kassertions.consume(topic, consumer)
                            .within(Duration.ofSeconds(10))
                            .untilAsserted(t -> t.hasSize(5))
            ).hasMessage(String.format("Timeout after 10000 ms while waiting for assertions on topic "
                                       + "'%s'. Failed assertions: Expected topic "
                                       + "to contain 5 records, but found 10.", topic));
        }
    }

}