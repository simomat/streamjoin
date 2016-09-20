package de.infonautika.streamjoin.join;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
            Function<L, Y> unmatchedLeft,
            Function<R, Y> unmatchedRight) {

        Clustered<K, R> rightCluster = right.collect(ClusteredCollector.clustered(rightKeyFunction));

        Function<L, ResultPart<K, Y>> mangledUnmatchedLeft = mangledUnmatchedLeft(unmatchedLeft);

        JoinFinished<K, Y> joinFinished = left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToLeftToResult<R, L, K, Y>) cluster -> l -> new ResultPart<>(key, grouper.apply(l, cluster)))
                                .orElse(mangledUnmatchedLeft))
                        .orElse(mangledUnmatchedLeft)
                        .apply(leftElement))
                .filter(result -> result != null)
                .collect(JoinFinished::new,
                        JoinFinished::accept,
                        JoinFinished::combine);


        if (unmatchedRight == null) {
            return joinFinished.result;
        }
        return Stream.concat(
                joinFinished.result,
                rightCluster.getMisfits(joinFinished.keys)
                        .map(unmatchedRight));

    }

    private static <K, Y, L> Function<L, ResultPart<K, Y>> mangledUnmatchedLeft(Function<L, Y> unmatchedLeft) {
        Function<L, ResultPart<K, Y>> mangledUnmatchedLeft;
        if (unmatchedLeft == null) {
            mangledUnmatchedLeft = l -> null;
        } else {
            mangledUnmatchedLeft = l -> new ResultPart<>(null, Stream.of(unmatchedLeft.apply(l)));
        }
        return mangledUnmatchedLeft;
    }

    private interface StreamToLeftToResult<R, L, K, Y> extends Function<Stream<R>, Function<L, ResultPart<K, Y>>> {}

    private static class ResultPart<K, Y> {
        private final K key;
        private final Stream<Y> joined;
        public ResultPart(K key, Stream<Y> joined) {
            this.key = key;
            this.joined = joined;
        }
    }

    private static class JoinFinished<K, Y>  {
        private final Set<K> keys = new HashSet<>();
        private Stream<Y> result = Stream.empty();
        public void accept(ResultPart<K, Y> resultPart) {
            if (resultPart.key != null) {
                keys.add(resultPart.key);
            }
            concatStream(resultPart.joined);
        }

        private void concatStream(Stream<Y> part) {
            result = Stream.concat(result, part);
        }

        public void combine(JoinFinished<K, Y> other) {
            keys.addAll(other.keys);
            concatStream(other.result);
        }
    }
}
