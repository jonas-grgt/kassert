package io.jonasg.kassert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TopicAssertions<K, V> {

    private final List<TopicAssertion<K, V>> assertions = new ArrayList<>();

    private boolean consumeUntilTimeout;

    /**
     * Asserts that the topic contains at least a record with the specified key and value.
     *
     * @param key
     *         the key to check for
     * @param value
     *         the value to check for
     * @return {@link TopicAssertions} for chaining assertions
     * @throws TopicAssertionError
     *         if the topic does not contain a record with the specified key and value
     */
    @SuppressWarnings("UnusedReturnValue")
    public TopicAssertions<K, V> contains(K key, V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key) &&
                                                    Objects.equals(record.value(), value)),
                        (__) -> new TopicAssertionError(
                                String.format(
                                        "Expected topic to contain key '%s' with value '%s', but was not found.",
                                        key, value))
                )
        );
        return this;
    }

    /**
     * Asserts that the topic contains exactly the specified number of records at the end of the duration of the timeout.
     * <p>
     * The timeout can be set using the {@link Kassertions#within(Duration)} method. If no timeout is specified, the
     * default timeout of {@link Kassertions#DEFAULT_TIMEOUT} (10 seconds) is used.
     * <p>
     * This method ensures that the consumer continues polling until the timeout is reached, even if the assertion
     * passes earlier.
     *
     * @param size
     *         the exact number of records expected in the topic
     * @return {@link TopicAssertions} for chaining additional assertions
     * @throws TopicAssertionError
     *         if the number of records in the topic does not match the expected size
     */
    @SuppressWarnings("UnusedReturnValue")
    public TopicAssertions<K, V> hasSize(int size) {
        this.consumeUntilTimeout = true;
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.size() == size,
                        (r) -> new TopicAssertionError(
                                String.format("Expected topic to contain %d records, but found %d.", size, r.size()))
                )
        );
        return this;
    }

    /**
     * Asserts that the topic contains at least a record with the specified key.
     *
     * @param key
     *         the key to check for
     * @throws TopicAssertionError if the topic does not contain a record with the specified key
     */
    public TopicAssertions<K, V> containsKey(V key) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key)),
                        (__) -> new TopicAssertionError(
                                String.format("Expected topic to contain key '%s', but was not found.", key))
                )
        );
        return this;
    }

    public void containsValue(V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.value(), value)),
                        (__) -> new TopicAssertionError(
                                String.format("Expected topic to contain value '%s', but was not found.", value))
                )
        );
    }

    List<TopicAssertionError> assertRecords(List<ConsumerRecord<K, V>> consumed) {
        return assertions.stream()
                .map(ka -> {
                    if (ka.assertion().apply(consumed)) {
                        return null;
                    } else {
                        return ka.errorSupplier().apply(consumed);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public boolean shouldConsumeUntilTimeout() {
        return consumeUntilTimeout;
    }
}
