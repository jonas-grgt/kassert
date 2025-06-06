# Usage
## Example usage 
Consume until a condition is met or fail after a timeout (5 seconds in this example):
```java
Kassertions.consume("topic", consumer)
    .within(Duration.ofSeconds(5))
        .untilAsserted(t -> t.containsKey(key));
```
## Size based assertions
The following will **continuously poll the topic for 5 seconds** unless the size is greater than 10, in which case it will fail immediately.
If by the end of the timeout the size is still not greater than 10, it will also fail otherwise if by then, exactly 10 records are present, it will pass.

```java
Kassertions.consume("topic", consumer)
    .within(Duration.ofSeconds(5)).hasSize(10);
```

This also applies to `hasSizeGreaterThan(int n)`, `hasSizeLessThan(int n)` and `isEmpty()`.

# Available Assertions

| Method                                                      | description                                                                                                                   |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `contains(K key, V value)`                                  | Asserts at least one record matches both key and value.                                                                       |
| `containsKey(K key)`                                        | Asserts a record with the given key is at least once present.                                                                 |
| `containsValue(V value)`                                    | Asserts a record with a given value is at least once present.                                                                 |
| `containsExactlyValues(List<V> expected)`                   | TODO                                                                                                                          |
| `containsExactlyKeys(List<V> expected)`                     | TODO                                                                                                                          |
| `containsKeysInAnyOrder(List<V> expected)`                  | Asserts a list of records with a given key is present in any order.                                                           |
| `containsValuesInAnyOrder(List<V> expected)`                | Asserts a list of records with a given value is present in any order.                                                         |
| `matches(Predicate<List<ConsumerRecord<K, V>>> predicate)`  | TODO                                                                                                                          |
| `hasKeySatisfying(Predicate<K> predicate)`                  | TODO                                                                                                                          |
| `hasValueSatisfying(Predicate<V> predicate)`                | TODO                                                                                                                          |
| `hasSize(int n)`                                            | Asserts that the topic contains exactly the specified number of records at the end of the duration of the timeout.            |
| `hasSizeGreaterThan(int n)`                                 | Asserts that the topic contains more than n records. Polling stops as soon as this condition is met, even before the timeout. |
| `hasSizeLessThan(int n)`                                    | TODO                                                                                                                          |
| `isEmpty()`                                                 | Asserts that the topic is empty at the end of the duration of the timeout.                                                    |
| `satisfies(Consumer<List<ConsumerRecord<K, V>>> assertion)` | TODO                                                                                                                          |
| `allSatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                                                                                          |
| `anySatisfy(Consumer<ConsumerRecord<K, V>> assertion)`      | TODO                                                                                                                          |