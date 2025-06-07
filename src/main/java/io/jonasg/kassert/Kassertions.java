package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Kassertions<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(Kassertions.class);

    static final long DEFAULT_TIMEOUT = 10_000;

    private final String topic;

    private final Consumer<K, V> consumer;

    private long timeout = DEFAULT_TIMEOUT;

    public static <K, V> Kassertions<K, V> consume(String topic, Consumer<K, V> consumer) {
        return new Kassertions<>(topic, consumer);
    }

    public Kassertions(String topic, Consumer<K, V> consumer) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be null or empty.");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer must not be null.");
        }
        this.topic = topic;
        this.consumer = consumer;
    }

    public Kassertions<K, V> within(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Timeout must be a positive duration.");
        }
        this.timeout = timeout.toMillis();
        return this;
    }

    public void untilAsserted(TopicAssertionBuilder<K, V> topicAssertionBuilder) {
        this.consumer.subscribe(List.of(this.topic));
        long remaining = this.timeout;

        List<ConsumerRecord<K, V>> consumed = new ArrayList<>();

        var topicAssertions = new TopicAssertions<K, V>();
        topicAssertionBuilder.build(topicAssertions);

        while (true) {
            long startPoll = System.currentTimeMillis();

            logger.debug("Polling ...");

            long pollDuration = Math.min(remaining, 1000);
            var records = this.consumer.poll(Duration.ofMillis(pollDuration));
            consumed.addAll(
                    StreamSupport.stream(records.spliterator(), false)
                            .collect(Collectors.toList()));

            logger.debug("Polled {} records", records.count());

            var errors = topicAssertions.assertRecords(consumed, timeout);

            if (errors.isEmpty() && !topicAssertions.shouldConsumeUntilTimeout()) {
                logger.debug("All assertions passed for topic: {}", this.topic);
                this.consumer.unsubscribe();
                break;
            }

            if (!errors.isEmpty() && topicAssertions.stopConsumptionOnError()) {
                logger.debug("Found {} errors for topic: {}", errors.size(), this.topic);
                throw topicAssertionError(errors);
            }

            remaining -= System.currentTimeMillis() - startPoll;
            if (remaining <= 0) {
                this.consumer.unsubscribe();

                // Some assertions such as hasSize() require to consume until timeout has been
                // reached.
                // If by then no errors have been found, we can exit without throwing an error.
                if (errors.isEmpty() && topicAssertions.shouldConsumeUntilTimeout()) {
                    break;
                }

                throw topicAssertionError(errors);
            }
        }
    }

    private TopicAssertionError topicAssertionError(List<TopicAssertionError> errors) {
        return new TopicAssertionError(
                String.format("Timeout after %d ms while waiting for assertions on topic '%s'. " +
                        "Failed assertions: %s", this.timeout, this.topic,
                        errors.stream().map(TopicAssertionError::getMessage)
                                .collect(Collectors.joining(", "))));
    }

}
