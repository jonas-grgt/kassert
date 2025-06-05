# Usage

```java
Kassertions.consume("topic", consumer)
    .within(Duration.ofSeconds(5))
        .untilAsserted(t -> t
            .hasSize(10)
            .contains(expectedKey, expectedValue));
```

# Available Assertions

| Method                                                      | description                                                                                                                   |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `contains(K key, V value)`                                  | Asserts at least one record matches both key and value.                                                                       |
| `containsKey(K key)`                                        | Asserts a record with the given key is at least once present.                                                                 |
| `containsValue(V value)`                                    | Asserts a record with a given value is at least once present.                                                                 |
| `containsExactly(List<V> expected)`                         | TODO                                                                                                                          |
| `containsInAnyOrder(List<V> expected)`                      | TODO                                                                                                                          |
| `matches(Predicate<List<ConsumerRecord<K, V>>> predicate)`  | TODO                                                                                                                          |
| `hasKeySatisfying(Predicate<K> predicate)`                  | TODO                                                                                                                          |
| `hasValueSatisfying(Predicate<V> predicate)`                | TODO                                                                                                                          |
| `hasSize(int n)`                                            | Asserts that the topic contains exactly the specified number of records at the end of the duration of the timeout.            |
| `hasSizeGreaterThan(int n)`                                 | Asserts that the topic contains more than n records. Polling stops as soon as this condition is met, even before the timeout. |
| `hasSizeLessThan(int n)`                                    | TODO                                                                                                                          |
| `isEmpty()`                                                 | TODO                                                                                                                          |
| `satisfies(Consumer<List<ConsumerRecord<K, V>>> assertion)` | TODO                                                                                                                          |
| `allSatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                                                                                          |
| `anySatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                                                                                          |