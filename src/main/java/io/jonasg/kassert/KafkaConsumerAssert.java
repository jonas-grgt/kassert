package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/// Asserts messages received by the consumer.
///
/// For example:
/// ```java
/// Kassertions.consume(consumer)
///    .assignedTo("orders", 0)
///    .filter(rec -> rec.key().equals("k1"))
///    .anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
/// ```
///
/// [#assignedTo] assigns the consumer; [#fromBeginning] (seek to the beginning), [#fromLast]
/// (seek n records back per partition) and [#within] (deadline, default 5 seconds) are
/// optional. The consumer is polled until the deadline is reached, accumulating every record
/// received across all polls. The terminal assertions ([#anySatisfy], [#allSatisfy],
/// [#noneSatisfy]) re-run against the [#filter]ed accumulated records on each poll and
/// return as soon as the outcome is decided; on timeout the failure is reported with the
/// observed records.
public final class KafkaConsumerAssert<K, V> implements
        KafkaConsumerAssertAssignmentStep<K, V>,
        KafkaConsumerAssertWaitStep<K, V>,
        KafkaConsumerAssertFilterStep<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerAssert.class);

    private final Consumer<K, V> consumer;

    private Duration timeout = Duration.ofSeconds(5);

    private boolean seekToBeginning;

    private Integer fromLast;

    private Predicate<ConsumerRecord<K, V>> filter = rec -> true;

    private KafkaConsumerAssert(Consumer<K, V> consumer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
    }

    /// Asserts messages received by the consumer.
    public static <K, V> KafkaConsumerAssertAssignmentStep<K, V> assertThat(Consumer<K, V> consumer) {
        return new KafkaConsumerAssert<>(consumer);
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> assignedTo(String topic) {
        return assignedTo(partitionsOf(topic));
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> assignedTo(String topic, int partition) {
        return assignedTo(List.of(new TopicPartition(topic, partition)));
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> assignedTo(Collection<TopicPartition> partitions) {
        consumer.assign(partitions);
        return this;
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> usingCurrentAssignment() {
        return this;
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> fromBeginning() {
        this.seekToBeginning = true;
        this.fromLast = null;
        return this;
    }

    @Override
    public KafkaConsumerAssertWaitStep<K, V> fromLast(int numberOfRecords) {
        if (numberOfRecords < 1) {
            throw new IllegalArgumentException("n must be >= 1 but was " + numberOfRecords);
        }
        this.fromLast = numberOfRecords;
        this.seekToBeginning = false;
        return this;
    }

    @Override
    public KafkaConsumerAssertFilterStep<K, V> within(long timeout, TimeUnit timeUnit) {
        this.timeout = Duration.ofNanos(timeUnit.toNanos(timeout));
        return this;
    }

    @Override
    public KafkaConsumerAssertFilterStep<K, V> filter(Predicate<ConsumerRecord<K, V>> predicate) {
        this.filter = this.filter.and(Objects.requireNonNull(predicate, "predicate cannot be null"));
        return this;
    }

    @Override
    public void anySatisfy(java.util.function.Consumer<ConsumerRecord<K, V>> assertion) {
        Objects.requireNonNull(assertion, "assertion");
        List<ConsumerRecord<K, V>> all = new ArrayList<>();
        long deadline = prepare();
        AssertionError lastError = null;
        do {
            pollOnce(all, deadline);
            for (ConsumerRecord<K, V> rec : filtered(all)) {
                try {
                    assertion.accept(rec);
                    return;
                } catch (AssertionError ex) {
                    lastError = ex;
                }
            }
        } while (System.nanoTime() < deadline);

        throw new AssertionError("Expected at least one matching record to satisfy the assertion within "
                + timeout + ", but none did. Matching records observed: ["
                + describeAll(filtered(all)) + "]", lastError);
    }

    @Override
    public void allSatisfy(java.util.function.Consumer<ConsumerRecord<K, V>> assertion) {
        Objects.requireNonNull(assertion, "assertion cannot be null");
        List<ConsumerRecord<K, V>> all = new ArrayList<>();
        long deadline = prepare();
        AssertionError lastError = null;
        ConsumerRecord<K, V> firstFailure = null;
        do {
            pollOnce(all, deadline);
            List<ConsumerRecord<K, V>> matching = filtered(all);
            boolean satisfied = !matching.isEmpty();
            for (ConsumerRecord<K, V> rec : matching) {
                try {
                    assertion.accept(rec);
                } catch (AssertionError e) {
                    lastError = e;
                    firstFailure = rec;
                    satisfied = false;
                }
            }
            if (satisfied) {
                return;
            }
        } while (System.nanoTime() < deadline);

        List<ConsumerRecord<K, V>> matching = filtered(all);
        if (matching.isEmpty()) {
            throw new AssertionError("Expected every matching record to satisfy the assertion within "
                    + timeout + ", but no records matched the filter.");
        }
        throw new AssertionError("Expected every matching record to satisfy the assertion within "
                + timeout + ", but record [" + describe(firstFailure) + "] did not. Matching records observed: ["
                + describeAll(matching) + "]", lastError);
    }

    @Override
    public void noneSatisfy(java.util.function.Consumer<ConsumerRecord<K, V>> assertion) {
        Objects.requireNonNull(assertion, "assertion");
        List<ConsumerRecord<K, V>> all = new ArrayList<>();
        long deadline = prepare();
        do {
            pollOnce(all, deadline);
            for (ConsumerRecord<K, V> rec : filtered(all)) {
                boolean satisfied = true;
                try {
                    assertion.accept(rec);
                } catch (AssertionError e) {
                    satisfied = false;
                }
                if (satisfied) {
                    throw new AssertionError("Expected no matching record to satisfy the assertion, but record ["
                            + describe(rec) + "] did.");
                }
            }
        } while (System.nanoTime() < deadline);
    }

    private long prepare() {
        if (seekToBeginning) {
            if (consumer.assignment().isEmpty()) {
                throw new IllegalStateException("fromBeginning() requires the consumer to be assigned");
            }
            consumer.seekToBeginning(consumer.assignment());
        } else if (fromLast != null) {
            if (consumer.assignment().isEmpty()) {
                throw new IllegalStateException("fromLast() requires the consumer to be assigned");
            }
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            for (var tp : consumer.assignment()) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                consumer.seek(tp, Math.max(start, end - fromLast));
            }
        }
        return System.nanoTime() + timeout.toNanos();
    }

    private void pollOnce(List<ConsumerRecord<K, V>> all, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining > 0) {
            ConsumerRecords<K, V> received = consumer.poll(Duration.ofNanos(Math.max(1, remaining)));
            received.forEach(all::add);
            log.debug("Received {} records so far", all.size());
        }
    }

    private List<ConsumerRecord<K, V>> filtered(List<ConsumerRecord<K, V>> all) {
        return all.stream().filter(filter).toList();
    }

    private static String describe(ConsumerRecord<?, ?> rec) {
        return rec.topic() + "-" + rec.partition() + "@" + rec.offset()
                + " key=" + rec.key() + " value=" + rec.value();
    }

    private static String describeAll(List<? extends ConsumerRecord<?, ?>> records) {
        return records.stream().map(KafkaConsumerAssert::describe).collect(Collectors.joining(", "));
    }

    private List<TopicPartition> partitionsOf(String topic) {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for topic '" + topic + "'");
        }
        return partitions.stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .toList();
    }
}
