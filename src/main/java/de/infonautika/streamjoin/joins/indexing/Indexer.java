package de.infonautika.streamjoin.joins.indexing;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class Indexer<T, K, D> {
    private static final Object NULLKEY = new Object();
    private final Stream<T> left;
    private final Function<T, K> leftKey;
    private Collector<T, ?, D> downstream;

    public Indexer(Stream<T> left, Function<T, K> leftKey, Collector<T, ?, D> downstream) {
        this.left = left;
        this.leftKey = leftKey;
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
        Map<K, D> leftKeyToLeft = collect(left, leftKey);

        collector.accept(
                leftKeyToLeft,
                leftKeyToLeft.remove(Indexer.<K>nullKey()));
    }

    private Map<K, D> collect(Stream<T> stream, Function<T, K> classifier) {
        return stream.collect(
                groupingBy(
                        nullTolerantClassifier(classifier),
                        downstream));
    }
}
