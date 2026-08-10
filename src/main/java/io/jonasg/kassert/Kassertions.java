package io.jonasg.kassert;

import org.apache.kafka.clients.consumer.Consumer;

public class Kassertions {
    public static <K, V> KafkaConsumerAssertAssignmentStep<K, V> consume(Consumer<K, V> consumer) {
        return KafkaConsumerAssert.assertThat(consumer);
    }
}
