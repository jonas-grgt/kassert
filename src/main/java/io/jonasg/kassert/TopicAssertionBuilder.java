package io.jonasg.kassert;

@FunctionalInterface
public interface TopicAssertionBuilder<K, V> {

    default void build(TopicAssertions<K, V> topicAssertions) {
        try {
            throwingBuild(topicAssertions);
        } catch (RuntimeException | AssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    void throwingBuild(TopicAssertions<K, V> e) throws Throwable;
}
