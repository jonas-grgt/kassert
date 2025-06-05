package io.jonasg.kassert;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;

class KAssertionHolder<K, V> {

    private final Function<List<ConsumerRecord<K, V>>, Boolean> assertion;

    private final Supplier<TopicAssertionError> errorSupplier;

    public KAssertionHolder(
            Function<List<ConsumerRecord<K, V>>, Boolean> assertion,
            Supplier<TopicAssertionError> errorSupplier) {
        this.assertion = assertion;
        this.errorSupplier = errorSupplier;
    }

    public Function<List<ConsumerRecord<K, V>>, Boolean> assertion() {
        return assertion;
    }

    public Supplier<TopicAssertionError> errorSupplier() {
        return errorSupplier;
    }
}
