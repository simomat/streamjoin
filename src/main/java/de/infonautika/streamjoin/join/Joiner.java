package de.infonautika.streamjoin.join;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class Joiner {

    public static <K, Y, L, R> Stream<Y> join(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<L, Y> unmatchedLeft,
            Function<R, Y> unmatchedRight) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();

        joinWithParameters(
                left,
                leftKeyFunction,
                right.collect(ClusteredCollector.clustered(rightKeyFunction)),
                (l, rs) -> builder.accept(grouper.apply(l, rs)),
                unmatchedLeft == null ? (l1) -> {} : l1 -> builder.accept(Stream.of(unmatchedLeft.apply(l1))),
                unmatchedRight == null ? (r) -> {} : r -> builder.accept(Stream.of(unmatchedRight.apply(r))));

        return builder.build()
                .flatMap(identity());
    }

    private static <L, R, K> void joinWithParameters(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Clustered<K, R> rightCluster,
            BiConsumer<L, Stream<R>> grouper,
            Consumer<L> unmatchedLeftConsumer,
            Consumer<R> unmatchedRightConsumer) {

        Function<L, K> leftUnmatched = l -> {
            unmatchedLeftConsumer.accept(l);
            return null;
        };

        Set<K> matchedKeys = left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToFunction<R, L, K>) cluster -> l -> {
                                    grouper.accept(l, cluster);
                                    return key;})
                                .orElse(leftUnmatched))
                        .orElse(leftUnmatched)
                        .apply(leftElement))
                .filter(matchedKey -> matchedKey != null)
                .collect(Collectors.toSet());

        rightCluster.getMisfits(matchedKeys)
                .forEach(unmatchedRightConsumer);

    }

    private interface StreamToFunction<R, L, K> extends Function<Stream<R>, Function<L, K>> {}
}
