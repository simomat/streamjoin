package de.infonautika.streamjoin.join;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

class ClusteredCollector<T, K> {
    private final Function<T, K> classifier;
    private final Map<K, List<T>> map;
    private final List<T> nullKeyElements;

    private ClusteredCollector(Function<T, K> classifier) {
        this(classifier, new HashMap<>(), new ArrayList<>());
    }

    private ClusteredCollector(Function<T, K> classifier, HashMap<K, List<T>> map, ArrayList<T> nullKeyElements) {
        this.classifier = classifier;
        this.map = map;
        this.nullKeyElements = nullKeyElements;
    }

    private void accept(T item) {
        withClusterOf(classifier.apply(item), cluster -> cluster.add(item));
    }

    private static <T, K> ClusteredCollector<T, K> combine(ClusteredCollector<T, K> first, ClusteredCollector<T, K> second) {
        return merge(copyOf(first), second);
    }

    private static <T, K> ClusteredCollector<T, K> merge(ClusteredCollector<T, K> receiver, ClusteredCollector<T, K> supplier) {
        supplier.map.forEach((key, otherCluster) ->
                receiver.withClusterOf(key, cluster -> cluster.addAll(otherCluster)));
        receiver.nullKeyElements.addAll(supplier.nullKeyElements);
        return receiver;
    }

    private static <T, K> ClusteredCollector<T, K> copyOf(ClusteredCollector<T, K> collector) {
        return new ClusteredCollector<>(
                collector.classifier,
                new HashMap<>(collector.map),
                new ArrayList<>(collector.nullKeyElements));
    }

    private Clustered<K, T> finish() {
        return new Clustered<>(map, nullKeyElements);
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


    static <T, K> Collector<T, ClusteredCollector<T, K>, Clustered<K, T>> clustered(Function<T, K> rightKeyFunction) {
        return new Collector<T, ClusteredCollector<T, K>, Clustered<K, T>>() {
            @Override
            public Supplier<ClusteredCollector<T, K>> supplier() {
                return () -> new ClusteredCollector<>(rightKeyFunction);
            }

            @Override
            public BiConsumer<ClusteredCollector<T, K>, T> accumulator() {
                return ClusteredCollector::accept;
            }

            @Override
            public BinaryOperator<ClusteredCollector<T, K>> combiner() {
                return ClusteredCollector::combine;
            }

            @Override
            public Function<ClusteredCollector<T, K>, Clustered<K, T>> finisher() {
                return ClusteredCollector::finish;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED);
            }
        };
    }


}
