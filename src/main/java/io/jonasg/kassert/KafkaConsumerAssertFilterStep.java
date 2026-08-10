package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;
import java.util.function.Predicate;

/// Narrows the records an assertion applies to and selects how the assertion terminates.
/// The terminal assertions run within the deadline set by [within], or the default of
/// 5 seconds when it was omitted.
public interface KafkaConsumerAssertFilterStep<K, V> {

    /// Restricts the assertion to records matching the given predicate.
    /// Can be chained; multiple filters are combined with a logical AND.
    KafkaConsumerAssertFilterStep<K, V> filter(Predicate<ConsumerRecord<K, V>> predicate);

    /// Asserts that the consumer receives at least one record matching the [filter]
    /// that
    /// satisfies the given assertion.
    /// Passes as soon as such a record is observed; fails once the deadline is
    /// reached.
    void anySatisfy(Consumer<ConsumerRecord<K, V>> assertion);

    /// Asserts that every record matching the [#filter] satisfies the given
    /// assertion.
    /// Each poll re-checks all records accumulated so far; passes as soon as they
    /// all
    /// satisfy, and fails once the deadline is reached or no records were observed.
    void allSatisfy(Consumer<ConsumerRecord<K, V>> assertion);

    /// Asserts that no record matching the [filter] satisfies the given assertion.
    /// Fails as soon as a violating record is observed; passes once the deadline is
    /// reached.
    void noneSatisfy(Consumer<ConsumerRecord<K, V>> assertion);
}
