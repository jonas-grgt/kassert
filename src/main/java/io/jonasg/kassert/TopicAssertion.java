package io.jonasg.kassert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicAssertion<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(TopicAssertion.class);

    private final String topic;

    private final Consumer<K, V> consumer;

    private long timeout = 10_000;


    public TopicAssertion(String topic, Consumer<K,V> consumer) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be null or empty.");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer must not be null.");
        }
        this.topic = topic;
        this.consumer = consumer;
    }

    public TopicAssertion<K, V> within(Duration timeout) {
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

        while (true) {
            long startPoll = System.currentTimeMillis();

            logger.info("Polling ...");

            long pollDuration = Math.min(remaining, 1000);
            var records = this.consumer.poll(Duration.ofMillis(pollDuration));
            consumed.addAll(
                    StreamSupport.stream(records.spliterator(), false)
                            .collect(Collectors.toList())
            );

            var topicAssertions = new TopicAssertions<K,V>();
            topicAssertionBuilder.build(topicAssertions);

            var errors = topicAssertions.assertRecords(consumed);

            if (errors.isEmpty()) {
                logger.info("All assertions passed for topic: {}", this.topic);
                this.consumer.unsubscribe();
                break;
            }

            remaining -= System.currentTimeMillis() - startPoll;
            if (remaining <= 0) {
                this.consumer.unsubscribe();
                throw new TopicAssertionError(
                        String.format("Timeout after %d ms while waiting for assertions on topic '%s'. " +
                                      "Failed assertions: %s", this.timeout, this.topic,
                                errors.stream().map(TopicAssertionError::getMessage).collect(Collectors.joining(", "))));
            }
        }
    }
}
