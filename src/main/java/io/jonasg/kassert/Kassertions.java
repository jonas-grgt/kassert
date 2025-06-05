package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.Consumer;

public class Kassertions {

    public static <K, V> TopicAssertion<K, V> consume(String topic, Consumer<K, V> consumer) {
        return new TopicAssertion<>(topic, consumer);
    }
}
