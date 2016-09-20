package de.infonautika.streamjoin.join;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class Joiner {

    public static <K, Y, L, R> Stream<Y> join(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<L, Y> unmatchedLeft) {

        ClusteredCollector<K, R> rightCluster = right.collect(
                () -> new ClusteredCollector<>(rightKeyFunction),
                ClusteredCollector::accept,
                ClusteredCollector::combine);

        Function<L, Stream<Y>> mangledUnmatchedLeft = mangledUnmatchedLeft(unmatchedLeft);

        return left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToLeftToResult<R, L, Y>) cluster -> l -> grouper.apply(l, cluster))
                                .orElse(mangledUnmatchedLeft))
                        .orElse(mangledUnmatchedLeft)
                        .apply(leftElement))
                .filter(result -> result != null)
                .flatMap(Function.identity());
    }

    private static <Y, L> Function<L, Stream<Y>> mangledUnmatchedLeft(Function<L, Y> unmatchedLeft) {
        if (unmatchedLeft == null) {
            return l -> null;
        }
        return l -> Stream.of(unmatchedLeft.apply(l));
    }

    private interface StreamToLeftToResult<R, L, Y> extends Function<Stream<R>, Function<L, Stream<Y>>> {}

}
