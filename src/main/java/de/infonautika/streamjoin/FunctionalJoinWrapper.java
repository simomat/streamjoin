package de.infonautika.streamjoin;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class FunctionalJoinWrapper {
    public static <Y, L, R, K> Stream<Y> joinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();

        joinWithConsumers(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                (l, rs) -> builder.accept(grouper.apply(l, rs)),
                (l1) -> {},
                (r) -> {}
        );

        return builder.build()
                .flatMap(identity());
    }

    public static <Y, L, R, K> Stream<Y> joinWithCombiner(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, R, Y> combiner) {

        return joinWithGrouper(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                (leftElement, rightElements) ->
                        rightElements.map(rightElement -> combiner.apply(leftElement, rightElement)));
    }

    public static <Y, L, R, K> Stream<Y> leftOuterJoinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<L, Y> unmatchedLeft) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();

        joinWithConsumers(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                (l, rs) -> builder.accept(grouper.apply(l, rs)),
                l1 -> builder.accept(Stream.of(unmatchedLeft.apply(l1))),
                (r) -> {}
        );

        return builder.build()
                .flatMap(identity());
    }


    public static <Y, L, R, K> Stream<Y> fullOuterJoinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<L, Y> unmatchedLeft,
            Function<R, Y> unmatchedRight) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();

        joinWithConsumers(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                (l1, rs) -> builder.accept(grouper.apply(l1, rs)),
                l -> builder.accept(Stream.of(unmatchedLeft.apply(l))),
                r -> builder.accept(Stream.of(unmatchedRight.apply(r))));

        return builder.build()
                .flatMap(identity());
    }

    private static <L, R, K> void joinWithConsumers(Stream<L> left, Function<L, K> leftKeyFunction, Stream<R> right, Function<R, K> rightKeyFunction, BiConsumer<L, Stream<R>> consumer, Consumer<L> unmatchedLeftConsumer, Consumer<R> unmatchedRightConsumer) {
        FunctionalJoin.joinWithClusterAndConsumers(
                left,
                leftKeyFunction,
                right.collect(
                        () -> new Clustered<>(rightKeyFunction),
                        Clustered::accept,
                        Clustered::combine),
                consumer,
                unmatchedLeftConsumer,
                unmatchedRightConsumer
        );
    }
}
