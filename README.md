# streamjoin

... for SQL-like Java 8 Stream joins, inspired by C# Enumerable.Join().

It correlates the elements of two streams and provides transformation of matching objects by passing a BiFunction. The correlation between two objects is established by values of key functions.   

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

#### Matching by other constraints

By default, a match is established by equality of key values. Matching by other constraints is provided:
```java
Stream<ShowAttendance> attendances = Join.
    join(listOfPersons.stream())
      .withKey(Person::getAge)
      .on(listOfShows.stream())
      .withKey(Show::getMinAge)
      .matching((personAge, minAge) -> personAge >= minAge)
      .combine((person, show) -> new ShowAttendance(show, person))
      .asStream();
```

#### Not matching Objects and key functions returning null

`Join.join(...)` defines an inner join, meaning that objects which do not correlate at all are not handled by the combiner and thus will not appear in the result.

Key functions which return `null` for one or many objects are tollerated, but will treat the object as not matchable.

#### Join Types

- [inner join](https://en.wikipedia.org/wiki/Join_(SQL)#Inner_join) as shown with `Join.join(...)`
- [left outer joins](https://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join) with `Join.leftJoin(...)`.
Unmatching objects of the left side (i.e. the first stream given) are respected. By default, `null` will be passed to the combining function. An additional handler for unmatching left side objects can be defined with 
```java
    .combine((left, right) -> something(left, right))
    .withLeftUnmatched(left -> someOther(left))
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

The left side stream is handled lazy and is not 'consumed', e.g. no [terminal operation](https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps) is performed on it.

The right side input stream is collected when finishing the join call. References on resulting data of that stream are held in memory until the resulting joined stream is 'consumed'.

Hence, if huge streams are joined and memory efficiency matters, using the 'shorter' input stream as right side should be considered.

