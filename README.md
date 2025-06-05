# Usage

```java
Kassertions.consume("topic", consumer)
    .within(Duration.ofSeconds(5))
        .untilAsserted(t -> t.contains(expectedKey, expectedValue));
```

# Available Assertions

| Method                                                      | description                                             |
|-------------------------------------------------------------|---------------------------------------------------------|
| `contains(K key, V value)`                                  | Asserts at least one record matches both key and value. |
| `containsKey(K key)`                                        | Asserts a record with the given key is present.         |
| `containsValue(V value)`                                    | TODO                                                    |
| `containsExactly(List<ConsumerRecord<K, V>> expected)`      | TODO                                                    |
| `containsInAnyOrder(List<ConsumerRecord<K, V>> expected)`   | TODO                                                    |
| `matches(Predicate<List<ConsumerRecord<K, V>>> predicate)`  | TODO                                                    |
| `hasKeySatisfying(Predicate<K> predicate)`                  | TODO                                                    |
| `hasValueSatisfying(Predicate<V> predicate)`                | TODO                                                    |
| `hasSize(int n)`                                            | TODO                                                    |
| `hasSizeGreaterThan(int n)`                                 | TODO                                                    |
| `hasSizeLessThan(int n)`                                    | TODO                                                    |
| `isEmpty()`                                                 | TODO                                                    |
| `satisfies(Consumer<List<ConsumerRecord<K, V>>> assertion)` | TODO                                                    |
| `allSatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                    |
| `anySatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                    |
