package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.joins.indexing.Indexer;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;


public class FullOuterJoin<L, R, K, Y> extends LeftOuterJoin<L, R, K, Y> {

    private Set<K> unmatchedRight;
    private final L leftNull = null;

    public FullOuterJoin(Indexer<L, R, K> indexer, BiFunction<L, Stream<R>, Stream<Y>> grouper) {
        super(indexer, grouper);
    }

    @Override
    protected void keyMatched(K lKey) {
        super.keyMatched(lKey);
        unmatchedRight.remove(lKey);
    }

    @Override
    protected void buildUnmatchedKeySet() {
        super.buildUnmatchedKeySet();
        unmatchedRight = map.rightKeySet();
    }

    @Override
    protected void handleKeyNullElements() {
        super.handleKeyNullElements();
        addNullMatchRight(map.getRightNullKeyElements());
    }

    private void addNullMatchRight(List<R> rightKeyNullElements) {
        addResultPart(grouper.apply(leftNull, rightKeyNullElements.stream()));
    }

    @Override
    protected void handleUnmatched() {
        super.handleUnmatched();
        unmatchedRight.forEach(key -> addNullMatchRight(map.getRight(key)));
    }

}
