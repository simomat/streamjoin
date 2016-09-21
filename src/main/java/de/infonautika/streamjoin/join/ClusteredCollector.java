package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

class ClusteredCollector<K, T> {
    private final Function<? super T, K> classifier;
    private final Map<K, List<T>> map;

    ClusteredCollector(Function<? super T, K> classifier) {
        this(classifier, new HashMap<>());
    }

    private ClusteredCollector(Function<? super T, K> classifier, HashMap<K, List<T>> map) {
        this.classifier = classifier;
        this.map = map;
    }

    Optional<Stream<T>> getCluster(Object key) {
        return Optional.ofNullable(map.get(key))
                .map(Collection::stream);
    }

    void accept(T item) {
        withClusterOf(classifier.apply(item), cluster -> cluster.add(item));
    }

    void combine(ClusteredCollector<K, T> source) {
        source.map.forEach((key, otherCluster) ->
                withClusterOf(key, cluster -> cluster.addAll(otherCluster)));
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
