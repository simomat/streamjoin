# streamjoin

... for SQL-like Java 8 Stream joins, inspired by C# Enumerable.Join().

 [ ![Download](https://api.bintray.com/packages/simomat/maven/streamjoin/images/download.svg) ](https://bintray.com/simomat/maven/streamjoin/_latestVersion) 

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

#### Not matching Objects and key functions returning null

`Join.join(...)` defines an inner join, meaning that objects which do not correlate at all are not handled by the combiner and thus will not appear in the result.

Key functions which return `null` for one or many objects are tolerated, but will treat the object as not matchable.

#### Join Types

- [inner join](https://en.wikipedia.org/wiki/Join_(SQL)#Inner_join) as shown with `Join.join(...)`
- [left outer joins](https://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join) with `Join.leftOuter(...)`.
- [full outer joins](https://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join) with `FullJoin.fullJoin(...)`.

With a left join, all entries from the first stream are always included. By default, when an item in the first stream is not matched in the second, `null` will be passed to the combining function. An additional handler for unmatching left side objects can be defined with: 
```java
    .combine((left, right) -> something(left, right))
    .withLeftUnmatched(left -> someOther(left))
    ...
```


With a full join, all entries from both streams are included. Matching entries are paired up with the combiner.

```java

Stream<BestFriends> bestFriends = FullJoin.
    fullJoin(listOfPersons.stream())
      .withKey(Person::getName)
      .on(listOfDogs.stream())
      .withKey(Dog::getOwnerName)
      .combine((person, dog) -> new BestFriends(person, dog))
      .asStream();

```

Non-matching entries on the left and right are represented as null, or may be mapped using:

```java
    .withLeftUnmatched(left -> someOtherLeft(left))
    .withRightUnmatched(right -> someOtherRight(right))

```

#### One to Many, Many to One, Many to Many
For all join types, multiple matches are respected by calling the combiner for each match. For inner and left joins, instead of `.combine(combiner)`, a grouped matcher may be defined, that takes a left object and a stream of matching right objects as parameter:
```java
    ...
    .group((left, streamOfMatchingRight) -> something(left, streamOfMatchingRight))
    ...
``` 

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

#### Parallel processing and performance
`streamjoin` supports parallel processing by just passing parallel streams (see [Collection.parallelStream()](https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html#parallelStream--) and [Stream.parallel()](https://docs.oracle.com/javase/8/docs/api/java/util/stream/BaseStream.html#parallel--)). In order to guarantee correctness, the key functions and combiner/grouper functions should be [non-interfering](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#NonInterference) and [stateless](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#Statelessness).

The left side stream is handled lazily and is not 'consumed', i.e. no [terminal operation](https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps) is performed on it.

The right side input stream is collected when finalizing the join with `.asStream()`. References on resulting data of that stream are held in memory until the resulting joined stream is 'consumed'.

Hence, if huge streams are joined and memory efficiency matters, using the 'shorter' input stream as right side should be considered.

#### Get it
`streamjoin` is available via jcenter:
```xml
<dependency>
    <groupId>de.infonautika.streamjoin</groupId>
    <artifactId>streamjoin</artifactId>
    <version>1.1.0</version>
    <type>pom</type>
</dependency>
```
or
```groovy
compile 'de.infonautika.streamjoin:streamjoin:1.1.0'
```
