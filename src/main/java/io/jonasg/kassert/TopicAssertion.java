package io.jonasg.kassert;

import java.util.List;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;

class TopicAssertion<K, V> {

    private final Function<List<ConsumerRecord<K, V>>, Boolean> assertion;

    private final Function<List<ConsumerRecord<K, V>>, TopicAssertionError> errorSupplier;

    public TopicAssertion(
            Function<List<ConsumerRecord<K, V>>, Boolean> assertion,
            Function<List<ConsumerRecord<K, V>>, TopicAssertionError> errorSupplier) {
        this.assertion = assertion;
        this.errorSupplier = errorSupplier;
    }

    public Function<List<ConsumerRecord<K, V>>, Boolean> assertion() {
        return assertion;
    }

    public Function<List<ConsumerRecord<K, V>>, TopicAssertionError> errorSupplier() {
        return errorSupplier;
    }
}
