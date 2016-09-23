package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

class ClusterCollector<K, T> {
    private final HashMap<K, List<T>> map = new HashMap<>();
    private Function<? super T, K> classifier;

    private ClusterCollector(Function<? super T, K> classifier) {
        this.classifier = classifier;
    }

    private ClusterCollector<K, T> combine(ClusterCollector<K, T> other) {
        other.map.forEach((key, otherCluster) ->
                withClusterOf(key, cluster -> cluster.addAll(otherCluster)));
        return this;
    }

    private void accept(T item) {
        withClusterOf(classifier.apply(item), cluster -> cluster.add(item));
    }

    private void withClusterOf(K key, Consumer<List<T>> consumer) {
        if (key != null) {
            consumer.accept(computeCluster(key));
        }
    }

    private List<T> computeCluster(K key) {
        return map.computeIfAbsent(key, (k) -> new ArrayList<>());
    }

    private HashMap<K,List<T>> getMap() {
        return map;
    }

    static <I, K, T> Collector<? super T, ClusterCollector<K, T>, Cluster<I, K, T>> toCluster(
            Function<? super T, K> classifier,
            BiPredicate<I, K> matchPredicate) {

        return new Collector<T, ClusterCollector<K, T>, Cluster<I, K, T>>() {

            @Override
            public Supplier<ClusterCollector<K, T>> supplier() {
                return () -> new ClusterCollector<>(classifier);
            }

            @Override
            public BiConsumer<ClusterCollector<K, T>, T> accumulator() {
                return ClusterCollector::accept;
            }

            @Override
            public BinaryOperator<ClusterCollector<K, T>> combiner() {
                return ClusterCollector::combine;
            }

            @Override
            public Function<ClusterCollector<K, T>, Cluster<I, K, T>> finisher() {
                return collector -> new Cluster<>(collector.getMap(), matchPredicate);
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED);
            }
        };

    }
}
