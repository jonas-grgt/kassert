package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopicAssertions<K, V> {

    private final List<TopicAssertion<K, V>> assertions = new ArrayList<>();

    private boolean consumeUntilTimeout;

    /**
     * Asserts that the topic contains at least a record with the specified key and
     * value.
     *
     * @param key
     *            the key to check for
     * @param value
     *            the value to check for
     * @return {@link TopicAssertions} for chaining assertions
     * @throws TopicAssertionError
     *             if the topic does not contain a record with the specified key and
     *             value
     */
    @SuppressWarnings("UnusedReturnValue")
    public TopicAssertions<K, V> contains(K key, V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key) &&
                                        Objects.equals(record.value(), value)),
                        (r, t) -> new TopicAssertionError(
                                String.format(
                                        "Expected topic to contain key '%s' with value '%s', but was not found.",
                                        key, value))));
        return this;
    }

    /**
     * Asserts that the topic contains exactly the specified number of records at
     * the end of the duration of the timeout.
     * <p>
     * The timeout can be set using the {@link Kassertions#within(Duration)} method.
     * If no timeout is specified, the
     * default timeout of {@link Kassertions#DEFAULT_TIMEOUT} (10 seconds) is used.
     * <p>
     * This method ensures that the consumer continues polling until the timeout is
     * reached, even if the assertion
     * passes earlier.
     *
     * @param size
     *            the exact number of records expected in the topic
     * @return {@link TopicAssertions} for chaining additional assertions
     * @throws TopicAssertionError
     *             if the number of records in the topic does not match the expected
     *             size
     */
    @SuppressWarnings("UnusedReturnValue")
    public TopicAssertions<K, V> hasSize(int size) {
        this.consumeUntilTimeout = true;
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.size() == size,
                        (r, t) -> new TopicAssertionError(
                                String.format("Expected topic to contain %d records, but found %d.", size, r.size()))));
        return this;
    }

    /**
     * Asserts that the topic contains more than n records. Polling stops as soon as
     * this condition is met, even before the timeout.
     * 
     * @param size
     *            the minimum number of records expected in the topic
     */
    public void hasSizeGreaterThan(int size) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.size() > size,
                        (r, t) -> new TopicAssertionError(
                                String.format(
                                        "Expected topic to contain more than %d records, but only found %d after %d ms.",
                                        size, r.size(), t))));
    }

    /**
     * Asserts that the topic contains at least a record with the specified key.
     *
     * @param key
     *            the key to check for
     * @throws TopicAssertionError
     *             if the topic does not contain a record with the specified key
     */
    public TopicAssertions<K, V> containsKey(V key) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key)),
                        (r, t) -> new TopicAssertionError(
                                String.format("Expected topic to contain key '%s', but was not found.", key))));
        return this;
    }

    public void containsValue(V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.value(), value)),
                        (r, t) -> new TopicAssertionError(
                                String.format("Expected topic to contain value '%s', but was not found.", value))));
    }

    /**
     * Asserts that the topic is empty at the end of the duration of the timeout.
     * 
     * @throws TopicAssertionError
     *             if the topic is not empty
     */
    public TopicAssertions<K, V> isEmpty() {
        this.consumeUntilTimeout = true;
        this.assertions.add(
                new TopicAssertion<>(
                        List::isEmpty,
                        (r, t) -> new TopicAssertionError(
                                String.format("Expected topic to be empty, but found %d records.", r.size()))));
        return this;
    }

    List<TopicAssertionError> assertRecords(List<ConsumerRecord<K, V>> consumed, long timeout) {
        return assertions.stream()
                .map(ka -> {
                    if (ka.assertion().apply(consumed)) {
                        return null;
                    } else {
                        return ka.errorSupplier().apply(consumed, timeout);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    boolean shouldConsumeUntilTimeout() {
        return consumeUntilTimeout;
    }
}
