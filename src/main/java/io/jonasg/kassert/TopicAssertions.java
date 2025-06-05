package io.jonasg.kassert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TopicAssertions<K, V> {

    private final List<TopicAssertion<K, V>> assertions = new ArrayList<>();

    /**
     * Asserts that the topic contains at least a record with the specified key and value.
     *
     * @param key
     *         the key to check for
     * @param value
     *         the value to check for
     * @return {@link TopicAssertions} for chaining assertions
     */
    @SuppressWarnings("UnusedReturnValue")
    public TopicAssertions<K, V> contains(K key, V value) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key) &&
                                                    Objects.equals(record.value(), value)),
                        () -> new TopicAssertionError(
                                String.format(
                                        "Expected topic to contain key '%s' with value '%s', but was not found.",
                                        key, value))
                )
        );
        return this;
    }

    /**
     * Asserts that the topic contains at least a record with the specified key.
     *
     * @param key
     *         the key to check for
     */
    public void containsKey(V key) {
        this.assertions.add(
                new TopicAssertion<>(
                        r -> r.stream()
                                .anyMatch(record -> Objects.equals(record.key(), key)),
                        () -> new TopicAssertionError(
                                String.format("Expected topic to contain key '%s', but was not found.", key))
                )
        );
    }

    List<TopicAssertionError> assertRecords(List<ConsumerRecord<K, V>> consumed) {
        return assertions.stream()
                .map(ka -> {
                    if (ka.assertion().apply(consumed)) {
                        return null;
                    } else {
                        return ka.errorSupplier().get();
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
