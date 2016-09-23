package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Cluster<I, K, T> {
    private final Function<I, Optional<Stream<T>>> clusterResolver;

    // Dependency on HashMap instead of Map, because the concrete implementation guarantees not to throw
    // ClassCastException on HashMap.get(key) when the type of key differs from K in contradiction to Map.get(key) spec.
    public Cluster(HashMap<K, List<T>> map, BiPredicate<I, K> matchPredicate) {
        this.clusterResolver = createClusterResolver(map, matchPredicate);
    }

    Optional<Stream<T>> getCluster(I key) {
        return clusterResolver.apply(key);
    }

    private static <I, K, T> Function<I, Optional<Stream<T>>> createClusterResolver(HashMap<K, List<T>> map, BiPredicate<I, K> matchPredicate) {
        if (matchPredicate == MatchPredicate.EQUALS) {
            //noinspection SuspiciousMethodCalls
            return key -> Optional.ofNullable(map.get(key))
                    .map(Collection::stream);
        }
        return key -> emptyIfStreamIsEmpty(map.entrySet().stream()
                .filter(es -> matchPredicate.test(key, es.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream));
    }

    static <T> Optional<Stream<T>> emptyIfStreamIsEmpty(Stream<T> stream) {
        Iterator<T> iterator = stream.iterator();
        if (iterator.hasNext()) {
            return Optional.of(StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false));
        }
        return Optional.empty();
    }

}
