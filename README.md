# streamjoin

... for SQL-like Java 8 Stream joins, inspired by C# Enumerable.Join().

It correlates the elements of two streams and provides transformation of matching objects by passing a BiFunction. The correlation between two objects is established by equality of values defined by functions. 

Joins are applied using a fluent API:
```java
Stream<BestFriends> bestFriends = Join.
    join(listOfPersons.stream())
      .withKey(Person::getName)
      .on(listOfDogs.stream())
      .withKey(Dog::getOwnerName)
      .combine((person, dog) -> new BestFriends(person, dog))
      .asStream();
```

This combines `Person` objects with `Dog` objects by matching equality of a name property and creates a result object for each match.

#### Not matching Objects and key functions returning null

`Join.join(...)` defines an inner join, meaning that objects which do not correlate at all are not handled by the combiner and thus will not appear in the result.

Key functions which return `null` for one or many objects are tollerated, but will treat the object as not matchable.

#### Join Types

- inner join as shown with `Join.join(...)`
- left outer joins with `Join.leftJoin(...)`.
Unmatching objects of the left side (i.e. the first stream given) are respected. By default, `null` will be passed to the combining function. An additional handler for unmatching left side objects can be defined with 
```java
    .combine((left, right) -> something(left, right))
    .withLeftUnmatched(left -> someOther(left))
    ...
```
- full outer joins with `Join.fullOuter(...)`.
Unmatching objects of left and right side are respected. `null` will be passed to the combiner by default. Optional handlers for left and/or right unmatching objects are definable. Use
```java
    .combine((left, right) -> something(left, right))
    .withLeftUnmatched(left -> someOther(left))
    .withRightUnmatched(right -> somethingDifferent(right))
    ...
``` 

#### One to Many, Many to One, Many to Many
For all join types, multiple matches are respected by calling the combiner for each match. Instead of `.combine(combiner)`, a grouped matcher may be defined, that takes a left object and a stream of matching right object as parameter:
```java
    ...
    .group((left, streamOfMatchingRight) -> something(left, streamOfMatchingRight))
    ...
``` 

#### Parallel processing and performance
`streamjoin` supports parallel processing by just passing parallel streams (see [Collection.parallelStream()](https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html#parallelStream--) and [Stream.parallel()](https://docs.oracle.com/javase/8/docs/api/java/util/stream/BaseStream.html#parallel--)). In order to guarantee correctness, the key functions and combiner/grouper functions should be [non-interfering](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#NonInterference) and [stateless](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#Statelessness).



#### Ideas for next steps
- [ ] add non-equi join (like 'WHERE A.RANK < B.RANK')
- [ ] return a result without 'consume' the left side stream with a terminal operation
- [ ] make generic types less restrictive with bounded wildcards

