package de.infonautika.streamjoin.join;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class JoinWrapper {

    public static <K, Y, L, R> Stream<Y> joinWithParameters(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<L, Y> unmatchedLeft,
            Function<R, Y> unmatchedRight) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();

        FunctionalJoin.join(
                left,
                leftKeyFunction,
                right.collect(ClusteredCollector.clustered(rightKeyFunction)),
                (l, rs) -> builder.accept(grouper.apply(l, rs)),
                unmatchedLeft == null ? (l1) -> {} : l1 -> builder.accept(Stream.of(unmatchedLeft.apply(l1))),
                unmatchedRight == null ? (r) -> {} : r -> builder.accept(Stream.of(unmatchedRight.apply(r)))
        );

        return builder.build()
                .flatMap(identity());

    }
}
