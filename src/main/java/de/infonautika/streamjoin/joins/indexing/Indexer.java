package de.infonautika.streamjoin.joins.indexing;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class Indexer<T, K, D> {
    private static final Object NULLKEY = new Object();
    private final Stream<T> elements;
    private final Function<T, K> classifier;
    private Collector<T, ?, D> downstream;

    public Indexer(Stream<T> elements, Function<T, K> classifier, Collector<T, ?, D> downstream) {
        this.elements = elements;
        this.classifier = classifier;
        this.downstream = downstream;
    }

    private static <T> T nullKey() {
        //noinspection unchecked
        return (T) NULLKEY;
    }

    private static <T,K> Function<T, K> nullTolerantClassifier(Function<T, K> classifier) {
        return l -> {
            K key = classifier.apply(l);
            if (key == null) {
                return nullKey();
            }
            return key;
        };
    }

    public void consume(BiConsumer<Map<K, D>, D> collector) {
        Map<K, D> leyToCollected = collect(elements, classifier);

        collector.accept(
                leyToCollected,
                leyToCollected.remove(Indexer.<K>nullKey()));
    }

    private Map<K, D> collect(Stream<T> stream, Function<T, K> classifier) {
        return stream.collect(
                groupingBy(
                        nullTolerantClassifier(classifier),
                        downstream));
    }
}
