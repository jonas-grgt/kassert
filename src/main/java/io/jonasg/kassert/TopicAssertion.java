package io.jonasg.kassert;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;

class TopicAssertion<K, V> {

    private final Function<List<ConsumerRecord<K, V>>, Boolean> assertion;

    private final BiFunction<List<ConsumerRecord<K, V>>, Long, TopicAssertionError> errorSupplier;

    public TopicAssertion(
            Function<List<ConsumerRecord<K, V>>, Boolean> assertion,
            BiFunction<List<ConsumerRecord<K, V>>, Long, TopicAssertionError> errorSupplier) {
        this.assertion = assertion;
        this.errorSupplier = errorSupplier;
    }

    public Function<List<ConsumerRecord<K, V>>, Boolean> assertion() {
        return assertion;
    }

    public BiFunction<List<ConsumerRecord<K, V>>, Long, TopicAssertionError> errorSupplier() {
        return errorSupplier;
    }
}
