package de.infonautika.streamjoin.joins.indexing;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public class DataMap<L, R, K> {
    private final Map<K, Stream<L>> leftKeyToLeft;
    private final Map<K, List<R>> rightKeyToRight;
    private final Stream<L> leftNull;
    private final List<R> rightNull;

    DataMap(Map<K, Stream<L>> leftKeyToLeft, Map<K, List<R>> rightKeyToRight, Stream<L> leftNull, List<R> rightNull) {
        this.leftKeyToLeft = leftKeyToLeft;
        this.rightKeyToRight = rightKeyToRight;
        this.leftNull = leftNull == null ? Stream.empty() : leftNull;
        this.rightNull = rightNull == null ? emptyList() : rightNull;
    }

    public void forEachLeft(BiConsumer<K, Stream<L>> consumer) {
        leftKeyToLeft.forEach(consumer);
    }

    public List<R> getRight(K key) {
        return rightKeyToRight.get(key);
    }

    public Set<K> leftKeySet() {
        return new HashSet<>(leftKeyToLeft.keySet());
    }

    public Stream<L> getLeft(K key) {
        return leftKeyToLeft.get(key);
    }

    public Set<K> rightKeySet() {
        return new HashSet<>(rightKeyToRight.keySet());
    }

    public Stream<L> getLeftNullKeyElements() {
        return leftNull;
    }

    public List<R> getRightNullKeyElements() {
        return rightNull;
    }
}
