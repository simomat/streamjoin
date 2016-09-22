package de.infonautika.streamjoin.join;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class Joiner {

    public static <L, R, KL, KR, Y> Stream<Y> join(
            Stream<? extends L> left,
            Function<? super L, KL> leftKeyFunction,
            Stream<? extends R> right,
            Function<? super R, KR> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<? super L, ? extends Y> unmatchedLeft) {

        Cluster<KR, R> rightCluster = createCluster(right, rightKeyFunction);

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

    private static <R, KR> Cluster<KR, R> createCluster(Stream<? extends R> right, Function<? super R, KR> rightKeyFunction) {
        return right.collect(
                    () -> new Cluster<>(rightKeyFunction),
                    Cluster::accept,
                    Cluster::combine);
    }

    private static <Y, L> Function<L, Stream<Y>> mangledUnmatchedLeft(Function<? super L, ? extends Y> unmatchedLeft) {
        if (unmatchedLeft == null) {
            return l -> null;
        }
        return l -> Stream.of(unmatchedLeft.apply(l));
    }

    private interface StreamToLeftToResult<R, L, Y> extends Function<Stream<R>, Function<L, Stream<Y>>> {}

}
