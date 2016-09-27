package de.infonautika.streamjoin;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.StreamMatcher.isEmptyStream;
import static de.infonautika.streamjoin.StreamMatcher.isStreamOf;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;

public class SubtypingTest {

    List<A0> a0s = null;
    List<A1> a1s = null;
    List<A2> a2s = null;

    List<B2> b2s = null;

    Function<A1, Number> keyA1 = A1::getKey;
    Function<A2, Number> keyA2 = A2::getKey;
    Function<B2, Number> keyB2 = B2::getKey;

    BiFunction<A1, A1, Integer> sumOfA = (a, b) -> a.getKey().intValue() + b.getKey().intValue();
    BiFunction<A1, B1, Integer> sumOfAB = (a, b) -> a.getKey().intValue() + b.getKey().intValue();
    BiFunction<A1, A2, String> joinStrings = (a, b) -> a.getKey().toString() + b.getKey().toString();

    @Before
    public void setUp() throws Exception {
        a0s = Stream.of(1, 2).map(i -> new A0() {}).collect(toList());
        a1s = Stream.of(1, 2).map(i -> (A1) () -> i).collect(toList());
        a2s = Stream.of(1, 2).map(i -> (A2) () -> Integer.valueOf(i)).collect(toList());

        b2s = Stream.of(1, 2).map(i -> (B2) () -> i).collect(toList());

    }

    @Test
    public void keyLeftAcceptsSubtypeOfLeft() throws Exception {
        Stream<Integer> stream = Join.join(a2s.stream()).withKey(keyA1).on(a1s.stream()).withKey(keyA1).combine(sumOfA).asStream();

        assertThat(stream, isStreamOf(2, 4));
    }


    @Test
    public void keyRightAcceptsSubtypeOfRight() throws Exception {
        Stream<Integer> stream = Join.join(a1s.stream()).withKey(keyA1).on(a2s.stream()).withKey(keyA1).combine(sumOfA).asStream();

        assertThat(stream, isStreamOf(2, 4));
    }

    @Test
    public void keyFunctionTypesAreIndependent() throws Exception {
        Function<A1, String> k1 = Object::toString;

        Stream<String> stream1 = Join.join(a2s.stream()).withKey(keyA2).on(a2s.stream()).withKey(k1).combine(joinStrings).asStream();
        Stream<String> stream2 = Join.join(a2s.stream()).withKey(k1).on(a2s.stream()).withKey(keyA2).combine(joinStrings).asStream();

        assertThat(stream1, isEmptyStream());
        assertThat(stream2, isEmptyStream());
    }

    @Test
    public void combinersAcceptSubtypes() throws Exception {
        BiFunction<A1, B1, Y1> combiner = (a, b) -> (Y2) () -> sumOfAB.apply(a, b);
        Stream<Y1> stream1 = Join.join(a2s.stream()).withKey(keyA2).on(b2s.stream()).withKey(keyB2).combine(combiner).asStream();
        Stream<Y1> stream2 = Join.leftOuter(a2s.stream()).withKey(keyA2).on(b2s.stream()).withKey(keyB2).combine(combiner).asStream();

        assertThat(stream1.map(Y1::getKey), isStreamOf(2, 4));
        assertThat(stream2.map(Y1::getKey), isStreamOf(2, 4));
    }

    @Test
    public void unmatchedLeftAcceptsAndReturnsSubtype() throws Exception {
        Function<A1, Y2> unmatchedLeft = a -> (Y2) () -> a.getKey().intValue() + 3;
        BiFunction<A1, Stream<B2>, Y1> grouper = (a, b) -> (Y1) a::getKey;
        BiFunction<A1, B1, Y1> combiner = (a, b) -> (Y1) () -> sumOfAB.apply(a, b);
        Stream<Y1> stream1 = Join.leftOuter(a2s.stream()).withKey(keyA2).on(Stream.<B2>empty()).withKey(keyB2).combine(combiner).withLeftUnmatched(unmatchedLeft).asStream();
        Stream<Y1> stream2 = Join.leftOuter(a2s.stream()).withKey(keyA2).on(Stream.<B2>empty()).withKey(keyB2).group(grouper).withLeftUnmatched(unmatchedLeft).asStream();

        assertThat(stream1.map(Y1::getKey), isStreamOf(4, 5));
        assertThat(stream2.map(Y1::getKey), isStreamOf(4, 5));
    }

    interface A0 {
    }

    interface A1 extends A0 {
        Number getKey();
    }

    interface A2 extends A1 {
    }

    interface B0 {
    }

    interface B1 extends B0 {
        Number getKey();
    }

    interface B2 extends B1 {
    }


    interface Y0 {
    }

    interface Y1 extends Y0 {
        Number getKey();
    }

    interface Y2 extends Y1 {
    }
}
