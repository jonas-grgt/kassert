package io.jonasg.kassert;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/// Selects which partitions the consumer is assigned to before polling.
public interface KafkaConsumerAssertAssignmentStep<K, V> {

    /// Assigns all partitions of the given topic to the consumer.
    KafkaConsumerAssertWaitStep<K, V> assignedTo(String topic);

    /// Assigns a single partition of the given topic to the consumer.
    KafkaConsumerAssertWaitStep<K, V> assignedTo(String topic, int partition);

    /// Assigns the given partitions to the consumer.
    KafkaConsumerAssertWaitStep<K, V> assignedTo(Collection<TopicPartition> partitions);

    /// Uses the consumer's existing assignment.
    KafkaConsumerAssertWaitStep<K, V> usingCurrentAssignment();
}
