package de.infonautika.streamjoin;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class FunctionalJoin {
    public static <Y, L, R, K> Stream<Y> joinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();
        BiConsumer<L, Stream<R>> consumer = (l, rs) -> builder.accept(grouper.apply(l, rs));

        joinWithClusterAndConsumer(
                left,
                leftKeyFunction,
                right.collect(
                        () -> new Clustered<>(rightKeyFunction),
                        Clustered::accept,
                        Clustered::combine),
                consumer
        );

        return builder.build()
                .flatMap(identity());
    }

    private static <L, R, K> void joinWithClusterAndConsumer(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Clustered<R, K> rightCluster,
            BiConsumer<L, Stream<R>> grouper) {

        Function<L, K> leftUnmatched = (l) -> null;
        Consumer<R> rightUnmatchedConsumer = (r) -> {};

        Set<K> matchedKeys = left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToFunction<R, L, K>) cluster -> left1 -> {
                                    grouper.accept(left1, cluster);
                                    return key;
                                })
                                .orElse(leftUnmatched))
                        .orElse(leftUnmatched)
                        .apply(leftElement))
                .filter(matchedKey -> matchedKey != null)
                .collect(Collectors.toSet());

        rightCluster.getMisfits(matchedKeys)
                .forEach(rightUnmatchedConsumer);

    }

    private interface StreamToFunction<R, L, K> extends Function<Stream<R>, Function<L, K>> {}

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
}
