package de.infonautika.streamjoin.joins.indexing;

import de.infonautika.streamjoin.streamutils.StreamCollector;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.streamutils.StreamCollector.toStream;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class Indexer<L, R, K> {
    private static final Object NULLKEY = new Object();
    private final Stream<L> left;
    private final Function<L, K> leftKey;
    private final Stream<R> right;
    private final Function<R, K> rightKey;

    public Indexer(Stream<L> left, Function<L, K> leftKey, Stream<R> right, Function<R, K> rightKey) {
        this.left = left;
        this.leftKey = leftKey;
        this.right = right;
        this.rightKey = rightKey;
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

    public DataMap<L, R, K> getIndexedMap() {
        Map<K, Stream<L>> leftKeyToLeft = collect(left, leftKey, toStream());
        Map<K, List<R>> rightKeyToRight = collect(right, rightKey, toList());

        return new DataMap<>(
                leftKeyToLeft,
                rightKeyToRight,
                leftKeyToLeft.remove(Indexer.<K>nullKey()),
                rightKeyToRight.remove(Indexer.<K>nullKey()));
    }

    private <T, D> Map<K, D> collect(Stream<T> stream, Function<T, K> classifier, Collector<T, ?, D> downstream) {
        return stream.collect(
                groupingBy(
                        nullTolerantClassifier(classifier),
                        downstream));
    }
}
