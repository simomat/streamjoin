package de.infonautika.streamjoin;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class Clustered<T, K> {
    private Function<T, K> classifier;
    private Map<K, List<T>> map = new HashMap<>();
    private List<T> nullKeyElements = new ArrayList<>();
    public Clustered(Function<T, K> classifier) {
        this.classifier = classifier;
    }

    public void accept(T item) {
        withClusterOf(classifier.apply(item), cluster -> cluster.add(item));
    }

    public void combine(Clustered<T, K> other) {
        other.map.forEach(
                (key, otherCluster) -> withClusterOf(key, thisCluster -> thisCluster.addAll(otherCluster)));
        nullKeyElements.addAll(other.nullKeyElements);
    }


    private void withClusterOf(K key, Consumer<List<T>> consumer) {
        consumer.accept(computeCluster(key));
    }

    private List<T> computeCluster(K key) {
        if (key == null) {
            return nullKeyElements;
        }
        return map.computeIfAbsent(key, (k) -> new ArrayList<>());
    }

    public Optional<Stream<T>> getCluster(K key) {
        return Optional.ofNullable(map.get(key))
                .map(Collection::stream);
    }

    public Stream<T> getMisfits(Set<K> keys) {
        return Stream.concat(
                map.entrySet().stream()
                        .filter(e -> !keys.contains(e.getKey()))
                        .map(Map.Entry::getValue)
                        .flatMap(Collection::stream),
                nullKeyElements.stream());
    }
}
