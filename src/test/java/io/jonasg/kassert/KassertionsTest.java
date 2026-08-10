package io.jonasg.kassert;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class KassertionsTest {

    static final AtomicInteger TOPIC_SEQ = new AtomicInteger();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

    Admin brokerAdmin;

    KafkaProducer<Object, Object> producer;

    @BeforeEach
    void setup() {
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(Map.of(
                    BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        }
        if (this.brokerAdmin == null) {
            this.brokerAdmin = AdminClient
                    .create(Map.of(BOOTSTRAP_SERVERS_CONFIG, KassertionsTest.kafka.getBootstrapServers()));
        }
    }

    @AfterEach
    void closeConsumers() {
        consumers.forEach(KafkaConsumer::close);
        consumers.clear();
    }

    @Nested
    class AnySatisfy {

        @Test
        void passesWhenRecordAlreadyPresent() {
            String topic = newTopic();
            produce(topic, "k", "v1");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void passesWhenRecordArrivesAfterStart() {
            String topic = newTopic();
            produceAsync(topic, "k", "v1", 2000);

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(5, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void passesWhenOnlyOneOfManySatisfies() {
            String topic = newTopic();
            IntStream.range(0, 10)
                    .forEach(i -> produce(topic, "k%d".formatted(i), "v%d".formatted(i)));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> {
                        assertThat(rec.key()).isEqualTo("k5");
                        assertThat(rec.value()).isEqualTo("v5");
                    });
        }

        @Test
        void timesOutWhenNoRecordArrives() {
            String topic = newTopic();

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(2, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("none did")
                    .hasMessageContaining("Matching records observed");
        }

        @Test
        void timesOutWhenNoRecordMatchesFilter() {
            String topic = newTopic();
            produce(topic, "k1", "v1");

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(2, TimeUnit.SECONDS)
                    .filter(rec -> rec.key().equals("k2"))
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("none did");
        }
    }

    @Nested
    class AllSatisfy {

        @Test
        void passesWhenAllRecordsSatisfy() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v1");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void passesWhenRecordArrivesAfterStart() {
            String topic = newTopic();
            produceAsync(topic, "k", "v1", 500);

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void timesOutWhenOneRecordViolates() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v2");

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(2, TimeUnit.SECONDS)
                    .allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Expected every matching record")
                    .hasMessageContaining("Matching records observed");
        }

        @Test
        void failsWhenNoRecordMatchesFilter() {
            String topic = newTopic();
            produce(topic, "k", "v-other");

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(2, TimeUnit.SECONDS)
                    .filter(rec -> rec.value().equals("v1"))
                    .allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("no records matched the filter");
        }

        @Test
        void keepsPollingAfterFirstViolation() {
            String topic = newTopic();
            produceAsync(topic, "k1", "v-bad", 300);
            produceAsync(topic, "k2", "v-good", 900);

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-good")))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("did not");
        }
    }

    @Test
    void noneSatisfyPassesWhenNoRecordsArrive() {
        String topic = newTopic();

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(2, TimeUnit.SECONDS)
                .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void noneSatisfyPassesWhenRecordsArriveButNoneSatisfy() {
        String topic = newTopic();
        produce(topic, "k", "v1");

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(2, TimeUnit.SECONDS)
                .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));
    }

    @Test
    void noneSatisfyFailsFastWhenRecordAlreadySatisfies() {
        String topic = newTopic();
        produce(topic, "k", "v-bad");

        assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected no matching record");
    }

    @Test
    void noneSatisfyFailsWhenViolatingRecordArrivesLater() {
        String topic = newTopic();
        produceAsync(topic, "k", "v-bad", 500);

        assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected no matching record");
    }

    @Test
    void noneSatisfyPassesWithFilter() {
        String topic = newTopic();
        produce(topic, "k1", "v1");

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(2, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k2"))
                .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void filterRestrictsToMatchingRecords() {
        String topic = newTopic();
        produce(topic, "k1", "v1");
        produce(topic, "k2", "v2");

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k1"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k2"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));
    }

    @Test
    void filterChainingCombinesWithAnd() {
        String topic = newTopic();
        produce(topic, "k", "v-good");
        produce(topic, "k", "v-bad");

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k"))
                .filter(rec -> rec.value().equals("v-good"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v-good"));

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k"))
                .filter(rec -> rec.value().equals("v-bad"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad"));
    }

    @Test
    void filterAppliedBeforeAsyncArrival() {
        String topic = newTopic();
        produceAsync(topic, "k", "v1", 400);

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .within(3, TimeUnit.SECONDS)
                .filter(rec -> rec.key().equals("k"))
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void omitsFromBeginningAndWithin_readsNewlyArrivingRecords() {
        String topic = newTopic();
        produceAsync(topic, "k", "v1", 400);

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void usesFromBeginningWithoutWithin() {
        String topic = newTopic();
        produce(topic, "k", "v1");

        Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .fromBeginning()
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
    }

    @Test
    void defaultTimeoutAppliesWhenWithinOmitted() {
        String topic = newTopic();

        assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                .assignedTo(topic, 0)
                .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("none did");
    }

    @Nested
    class FromLast {

        @Test
        void singlePartition_readsOnlyLastNRecords() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v2");
            produce(topic, "k3", "v3");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(2)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v3"));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(2)
                    .within(3, TimeUnit.SECONDS)
                    .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void clampsToLogStartWhenTopicHasFewerRecordsThanN() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v2");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(5)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(5)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));
        }

        @Test
        void rejectsNBelowOne() {
            String topic = newTopic();

            assertThatThrownBy(() -> Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("n must be >= 1");
        }

        @Test
        void allPartitions_windBackPerPartition() {
            String topic = newTopic(2);
            produce(topic, 0, "k1", "v1");
            produce(topic, 0, "k2", "v2");
            produce(topic, 1, "k3", "v3");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic)
                    .fromLast(1)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic)
                    .fromLast(1)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v3"));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic)
                    .fromLast(1)
                    .within(3, TimeUnit.SECONDS)
                    .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }

        @Test
        void fromBeginningThenFromLast_lastCallWins() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v2");
            produce(topic, "k3", "v3");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .fromLast(1)
                    .within(3, TimeUnit.SECONDS)
                    .noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromBeginning()
                    .fromLast(1)
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v3"));
        }

        @Test
        void fromLastThenFromBeginning_lastCallWins() {
            String topic = newTopic();
            produce(topic, "k1", "v1");
            produce(topic, "k2", "v2");
            produce(topic, "k3", "v3");

            Kassertions.consume(newConsumer())
                    .assignedTo(topic, 0)
                    .fromLast(1)
                    .fromBeginning()
                    .within(3, TimeUnit.SECONDS)
                    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
        }
    }

    private String newTopic(int partitions) {
        String name = "topic-" + TOPIC_SEQ.incrementAndGet();
        try {
            this.brokerAdmin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all()
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return name;
    }

    private String newTopic() {
        return this.newTopic(1);
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(GROUP_ID_CONFIG, "kassert-" + System.nanoTime());
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumers.add(consumer);
        return consumer;
    }

    private void produce(String topic, String key, String value) {
        try {
            this.producer.send(new ProducerRecord<>(topic, key, value)).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void produce(String topic, int partition, String key, String value) {
        try {
            this.producer.send(new ProducerRecord<>(topic, partition, key, value)).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void produceAsync(String topic, String key, String value, long delayMillis) {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMillis);
                produce(topic, key, value);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
