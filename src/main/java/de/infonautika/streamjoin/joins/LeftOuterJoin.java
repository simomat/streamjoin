package de.infonautika.streamjoin.joins;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

public class LeftOuterJoin<L, R, K, Y> extends InnerEquiJoin<L, R, K, Y> {

    private Set<K> unmatchedLeft;
    private final List<R> nullList = singletonList((R) null);

    public LeftOuterJoin(Supplier<DataMap<L, R, K>> dataMapSupplier) {
        super(dataMapSupplier);
    }

    @Override
    protected void doJoin() {
        buildUnmatchedKeySet();
        super.doJoin();
        handleUnmatched();
        handleKeyNullElements();
    }

    @Override
    protected void consumeMatch(K lKey, Stream<L> leftElements, List<R> rightElements) {
        super.consumeMatch(lKey, leftElements, rightElements);
        keyMatched(lKey);
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

    protected void handleUnmatched() {
        unmatchedLeft.forEach(key -> addNullMatch(map.getLeft(key)));
    }

    private void addNullMatch(Stream<L> left) {
        consumer.accept(left, nullList);
    }
}
