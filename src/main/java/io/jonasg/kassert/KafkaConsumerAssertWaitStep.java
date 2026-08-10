package io.jonasg.kassert;

import java.util.concurrent.TimeUnit;

/// Positions the consumer and sets the deadline for the assertion.
/// Both methods are optional; the terminal assertions can be reached without calling either.
public interface KafkaConsumerAssertWaitStep<K, V> extends KafkaConsumerAssertFilterStep<K, V> {

    /// Seeks all assigned partitions to the beginning before polling.
    /// Optional: when omitted the consumer uses its default offset behavior — the
    /// group's
    /// committed offsets, else `auto.offset.reset` (default `latest`).
    KafkaConsumerAssertWaitStep<K, V> fromBeginning();

    /// Seeks every assigned partition numberOfRecords records before its end.
    /// Consumption then proceeds forward, so records produced after this call are
    /// also
    /// consumed within the assertion window.
    ///
    /// When omitted the consumer uses its default offset behavior, the group's
    /// committed offsets, else `auto.offset.reset` (default `latest`).
    ///
    /// Mutually exclusive with [#fromBeginning]; the last call wins.
    KafkaConsumerAssertWaitStep<K, V> fromLast(int numberOfRecords);

    /// Sets the overall deadline within which the assertion must succeed.
    ///
    /// Defaults to 5 seconds when omitted.
    KafkaConsumerAssertFilterStep<K, V> within(long timeout, TimeUnit timeUnit);
}
