package io.jonasg.kassert;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.kafka.KafkaContainer;

public interface KafkaContainerSupport {

    KafkaContainer container = new KafkaContainer("apache/kafka-native:3.8.0")
            .withReuse(true)
            .withExposedPorts(9092, 9093);

    @BeforeAll
    static void setup() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
