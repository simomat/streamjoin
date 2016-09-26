package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

class ClusterCollector {

    static <I, K, T> Collector<? super T, HashMap<K, List<T>>, Cluster<I, K, T>> toCluster(
            Function<? super T, K> classifier,
            BiPredicate<I, K> matchPredicate) {

        return new Collector<T, HashMap<K, List<T>>, Cluster<I, K, T>>() {

            @Override
            public Supplier<HashMap<K, List<T>>> supplier() {
                return HashMap::new;
            }

            @Override
            public BiConsumer<HashMap<K, List<T>>, T> accumulator() {
                return (map, item) -> Optional.ofNullable(classifier.apply(item))
                        .map(key -> map.computeIfAbsent(key, k -> new ArrayList<>()))
                        .ifPresent(cluster -> cluster.add(item));
            }

            @Override
            public BinaryOperator<HashMap<K, List<T>>> combiner() {
                return (map, other) -> {
                    other.forEach((key, otherCluster) ->
                        map.merge(key, otherCluster, (left, right) -> { left.addAll(right); return left; }));
                    return map;
                };
            }

            @Override
            public Function<HashMap<K, List<T>>, Cluster<I, K, T>> finisher() {
                return map -> new Cluster<>(map, matchPredicate);
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED);
            }
        };
    }
}
