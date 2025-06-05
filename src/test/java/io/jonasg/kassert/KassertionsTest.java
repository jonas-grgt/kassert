package io.jonasg.kassert;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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


    //        @Nested
//        class HasSize {
//
//            @Test
//            void assertsTopicHasExactSize() throws ExecutionException, InterruptedException, TimeoutException {
//                var key = UUID.randomUUID().toString();
//                producer.send(new ProducerRecord<>("contains-topic", key, "value"))
//                        .get(5, TimeUnit.SECONDS);
//
//                Kassertions.consume("contains-topic", consumer)
//                        .within(Duration.ofSeconds(5))
//                        .untilAsserted(t -> t.contains(key, "value"));
//            }
//
//            @Test
//            void doesNotContain() throws ExecutionException, InterruptedException, TimeoutException {
//                var key = UUID.randomUUID().toString();
//                producer.send(new ProducerRecord<>("never-contains-topic", key, "non-matching-value"))
//                        .get(5, TimeUnit.SECONDS);
//
//                assertThatThrownBy(() ->
//                        Kassertions.consume("never-contains-topic", consumer)
//                                .within(Duration.ofSeconds(5))
//                                .untilAsserted(t -> t.contains(key, "value"))
//                ).hasMessage(String.format("Timeout after 5000 ms while waiting for assertions on topic "
//                                           + "'never-contains-topic'. Failed assertions: Expected topic "
//                                           + "to contain key '%s' with value 'value', "
//                                           + "but was not found.", key));
//            }
//        }

    //    @Nested
    //    class Satisfies {
    //        @Test
    //        void satisfiesCondition() throws ExecutionException, InterruptedException, TimeoutException {
    //            producer.send(new ProducerRecord<>("satisfies-topic", "key", "value"))
    //                    .get(5, TimeUnit.SECONDS);
    //
    //            Kassertions.assertThat("satisfies-topic", consumer)
    //                    .within(Duration.ofSeconds(5))
    //                    .satisfies(cr -> assertThat(cr.key()).isEqualTo("key"))
    //                    .kassert();
    //        }
    //    }

}