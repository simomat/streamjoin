package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.join.ClusterCollector.toCluster;
import static java.util.stream.Collectors.*;
import static java.util.stream.Stream.concat;

public class Joiner {

    public static <L, R, KL, KR, Y> Stream<Y> join(
            Stream<? extends L> left,
            Function<? super L, KL> leftKeyFunction,
            Stream<? extends R> right,
            Function<? super R, KR> rightKeyFunction,
            BiPredicate<KL, KR> matchPredicate,
            BiFunction<L, Stream<R>, Stream<Y>> grouper,
            Function<? super L, ? extends Y> unmatchedLeft) {

        Cluster<KL, KR, R> rightCluster = createCluster(right, rightKeyFunction, matchPredicate);

        Function<L, Stream<Y>> mangledUnmatchedLeft = mangledUnmatchedLeft(unmatchedLeft);

        return left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToLeftToResult<R, L, Y>) cluster -> l -> grouper.apply(l, cluster))
                                .orElse(mangledUnmatchedLeft))
                        .orElse(mangledUnmatchedLeft)
                        .apply(leftElement))
                .filter(Objects::nonNull)
                .flatMap(Function.identity());
    }

    public static <L, R, KL, KR, Y> Stream<Y> fullJoin(
            Stream<? extends L> left,
            Function<? super L, KL> leftKeyFunction,
            Stream<? extends R> right,
            Function<? super R, KR> rightKeyFunction,
            BiPredicate<KL, KR> matchPredicate,
            BiFunction<? super L, ? super R, ? extends Y> combiner,
            Function<? super L, ? extends Y> unmatchedLeft,
            Function<? super R, ? extends Y> unmatchedRight) {

        BiPredicate<Optional<KL>, Optional<KR>> predicate = (okl, okr) -> okl.isPresent() && okr.isPresent() && matchPredicate.test(okl.get(), okr.get());

        Map<Optional<KL>, ? extends List<? extends L>> leftByKey = left.collect(groupingBy(leftKeyFunction.andThen(Optional::ofNullable)));
        Map<Optional<KR>, ? extends List<? extends R>> rightByKey = right.collect(groupingBy(rightKeyFunction.andThen(Optional::ofNullable)));

        if (rightByKey.size() <= 0) {
            return leftByKey.values().stream().flatMap(v -> v.stream().map(unmatchedLeft::apply));
        }

        @SuppressWarnings("ConstantConditions")
        List<LeftWithMatches<KR, L, R>> leftWithMatches = leftByKey
                .entrySet()
                .stream()
                .map(lkv -> new LeftWithMatches<KR, L, R>(
                                lkv.getValue(),
                                rightByKey
                                        .entrySet()
                                        .stream()
                                        .filter(rkv -> predicate.test(lkv.getKey(), rkv.getKey()))
                                        .collect(toMap(rkv -> rkv.getKey().get(), Map.Entry::getValue))
                        )
                )
                .collect(toList());

        Set<KR> rightKeysMatchingLeft = leftWithMatches.stream().flatMap(x -> x.getRights().keySet().stream()).collect(toSet());

        Stream<? extends Y> leftJoin = leftWithMatches.stream().flatMap(lkv ->
            lkv.getRights().size() > 0
            ?
            lkv.getLefts().stream().flatMap(l -> lkv.getRights().values().stream().flatMap(rs -> rs.stream().map(r -> combiner.apply(l, r))))
            :
            lkv.getLefts().stream().map(unmatchedLeft::apply));

        Stream<? extends Y> rightUnmatched = rightByKey
            .entrySet()
            .stream()
            .filter(kv -> !kv.getKey().isPresent() || !rightKeysMatchingLeft.contains(kv.getKey().get()))
            .flatMap(kv -> kv.getValue().stream().map(unmatchedRight::apply));

        return concat(leftJoin, rightUnmatched);
    }

    private static class LeftWithMatches<KR, L, R> {
        private final List<? extends L> lefts;
        private final Map<KR, List<? extends R>> rights;

        LeftWithMatches(List<? extends L> lefts, Map<KR, List<? extends R>> rights) {
            this.lefts = lefts;
            this.rights = rights;
        }

        List<? extends L> getLefts() {
            return lefts;
        }

        Map<KR, List<? extends R>> getRights() {
            return rights;
        }
    }

    private static <R, KL, KR> Cluster<KL, KR, R> createCluster(Stream<? extends R> right, Function<? super R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate) {
        return right.collect(toCluster(rightKeyFunction, matchPredicate));
    }

    private static <Y, L> Function<L, Stream<Y>> mangledUnmatchedLeft(Function<? super L, ? extends Y> unmatchedLeft) {
        if (unmatchedLeft == null) {
            return l -> null;
        }
        return l -> Stream.of(unmatchedLeft.apply(l));
    }

    private interface StreamToLeftToResult<R, L, Y> extends Function<Stream<R>, Function<L, Stream<Y>>> {}
}
