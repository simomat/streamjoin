package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.joins.indexing.Indexer;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class LeftOuterJoin<L, R, K, Y> extends InnerEquiJoin<L, R, K, Y> {

    private Set<K> unmatchedLeft;
    private final R rightNullSentinel = null;

    public LeftOuterJoin(Indexer<L, R, K> indexer, BiFunction<L, Stream<R>, Stream<Y>> grouper) {
        super(indexer, grouper);
    }

    @Override
    protected void doJoin() {
        buildUnmatchedKeySet();
        super.doJoin();
        handleUnmatched();
        handleKeyNullElements();
    }

    protected void buildUnmatchedKeySet() {
        unmatchedLeft = map.leftKeySet();
    }

    protected void keyMatched(K lKey) {
        unmatchedLeft.remove(lKey);
    }

    protected void handleKeyNullElements() {
        addNullMatch(map.getLeftNullKeyElements());
    }

    @Override
    protected void consumeMatch(K lKey, Stream<L> leftElements, List<R> rightElements) {
        super.consumeMatch(lKey, leftElements, rightElements);
        keyMatched(lKey);
    }

    protected void handleUnmatched() {
        unmatchedLeft.forEach(key -> addNullMatch(map.getLeft(key)));
    }

    private void addNullMatch(Stream<L> left) {
        addResultPart(left
                .map(l -> grouper.apply(l, Stream.of(rightNullSentinel)))
                .flatMap(identity()));
    }
}
