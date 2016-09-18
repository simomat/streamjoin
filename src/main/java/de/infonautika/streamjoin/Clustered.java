package de.infonautika.streamjoin;

import java.util.*;
import java.util.stream.Stream;

public class Clustered<T, K> {
    private final Map<K, List<T>> map;
    private final List<T> nullKeyElements;

    public Clustered(Map<K, List<T>> map, List<T> nullKeyElements) {
        this.map = map;
        this.nullKeyElements = nullKeyElements;
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
