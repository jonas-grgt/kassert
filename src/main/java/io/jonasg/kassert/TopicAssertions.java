package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopicAssertions<K, V> {

    private final List<TopicAssertion<K, V>> assertions = new ArrayList<>();

    private boolean consumeUntilTimeout;

    private boolean stopConsumptionOnError;

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
     * Asserts that the topic contains less than n records. Polling stops as soon as
     * this condition is not met, even before the timeout.
     * 
     * @param size
     *            the maximum number of records expected in the topic
     */
    public void hasSizeLessThan(int size) {
        this.consumeUntilTimeout = true;
        this.stopConsumptionOnError = true;
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.size() < size,
                        (r, t) -> new TopicAssertionError(
                                String.format(
                                        "Expected topic to contain less than %d records, but found %d after %d ms.",
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

    /**
     * Asserts that the topic contains at least a record with the specified value
     *
     * @param value
     *            the value to check for
     * @throws TopicAssertionError
     *             if the topic does not contain a record with the specified key
     */
    public TopicAssertions<K, V> containsValue(V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.value(), value)),
                        (r, t) -> new TopicAssertionError(
                                String.format("Expected topic to contain value '%s', but was not found.", value))));
        return this;
    }

    public TopicAssertions<K, V> containsExactlyKeys(List<V> expected) {
        throw new TopicAssertionError("Assertion not implemented yet");
    }

    public TopicAssertions<K, V> containsExactlyValues(List<V> expected) {
        throw new TopicAssertionError("Assertion not implemented yet");
    }

    /**
     * Asserts that the topic contains a list of records with the specified keys in
     * any order
     *
     * @param expected
     *            the keys to check for
     * @throws TopicAssertionError
     *             if the topic does not contain a record with the specified key
     */
    public TopicAssertions<K, V> containsKeysInAnyOrder(List<K> expected) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .map(ConsumerRecord::key)
                                .collect(Collectors.toSet())
                                .containsAll(expected),
                        (r, t) -> {
                            var missing = new HashSet<>(expected);

                            List<K> keys = r.stream()
                                    .map(ConsumerRecord::key)
                                    .collect(Collectors.toList());

                            keys.forEach(missing::remove);
                            return new TopicAssertionError(
                                    String.format(
                                            "Expected topic to contain keys %s in any order, but %s could not be found in received keys %s",
                                            expected, missing, keys));
                        }));
        return this;
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

    /**
     * Asserts that the topic contains a list of records with the specified values
     * in any order
     *
     * @param expected
     *            the values to check for
     * @throws TopicAssertionError
     *             if the topic does not contain a record with the specified key
     */
    public TopicAssertions<K, V> containsValuesInAnyOrder(List<V> expected) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .map(ConsumerRecord::value)
                                .collect(Collectors.toSet())
                                .containsAll(expected),
                        (r, t) -> {
                            var missing = new HashSet<>(expected);

                            List<V> values = r.stream()
                                    .map(ConsumerRecord::value)
                                    .collect(Collectors.toList());

                            values.forEach(missing::remove);
                            return new TopicAssertionError(
                                    String.format(
                                            "Expected topic to contain values %s in any order, but %s could not be found in received values %s",
                                            expected, missing, values));
                        }));
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

    public boolean stopConsumptionOnError() {
        return stopConsumptionOnError;
    }
}
