package io.jonasg.kassert;

public interface TopicAssertionBuilder<K, V> {
    void build(TopicAssertions<K, V> e);
}
