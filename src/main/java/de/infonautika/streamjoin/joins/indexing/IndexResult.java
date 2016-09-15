package de.infonautika.streamjoin.joins.indexing;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface IndexResult<L, R, K> {
    void accept(Map<K, Stream<L>> leftKeyToLeft, Map<K, List<R>> rightKeyToRight, Stream<L> leftNull, List<R> rightNull);
}
