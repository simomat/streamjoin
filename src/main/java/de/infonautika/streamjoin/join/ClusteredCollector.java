package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

class ClusteredCollector<K, T> {
    private final Function<T, K> classifier;
    private final Map<K, List<T>> map;

    ClusteredCollector(Function<T, K> classifier) {
        this(classifier, new HashMap<>());
    }

    private ClusteredCollector(Function<T, K> classifier, HashMap<K, List<T>> map) {
        this.classifier = classifier;
        this.map = map;
    }

    Optional<Stream<T>> getCluster(K key) {
        return Optional.ofNullable(map.get(key))
                .map(Collection::stream);
    }

    void accept(T item) {
        withClusterOf(classifier.apply(item), cluster -> cluster.add(item));
    }

    static <T, K> ClusteredCollector<K, T> combine(ClusteredCollector<K, T> first, ClusteredCollector<K, T> second) {
        return merge(copyOf(first), second);
    }

    private static <T, K> ClusteredCollector<K, T> merge(ClusteredCollector<K, T> receiver, ClusteredCollector<K, T> supplier) {
        supplier.map.forEach((key, otherCluster) ->
                receiver.withClusterOf(key, cluster -> cluster.addAll(otherCluster)));
        return receiver;
    }

    private static <T, K> ClusteredCollector<K, T> copyOf(ClusteredCollector<K, T> collector) {
        return new ClusteredCollector<>(
                collector.classifier,
                new HashMap<>(collector.map));
    }

    private void withClusterOf(K key, Consumer<List<T>> consumer) {
        if (key != null) {
            consumer.accept(computeCluster(key));
        }
    }

    private List<T> computeCluster(K key) {
        return map.computeIfAbsent(key, (k) -> new ArrayList<>());
    }
}
