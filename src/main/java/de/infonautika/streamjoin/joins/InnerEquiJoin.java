package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.consumer.MatchConsumer;
import de.infonautika.streamjoin.joins.indexing.DataMap;
import de.infonautika.streamjoin.joins.indexing.Indexer;

import java.util.List;
import java.util.stream.Stream;

public class InnerEquiJoin<L, R, K, Y> implements JoinStrategy<L, R, Y> {

    private final Indexer<L, R, K> indexer;
    protected MatchConsumer<L, R, Y> consumer;
    protected DataMap<L, R, K> map;

    public InnerEquiJoin(Indexer<L, R, K> indexer) {
        this.indexer = indexer;
    }

    @Override
    public void join(MatchConsumer<L, R, Y> consumer) {
        this.consumer = consumer;
        indexer.collectResult((leftKeyToLeft, rightKeyToRight, leftNull, rightNull) ->
                map = new DataMap<>(leftKeyToLeft, rightKeyToRight, leftNull, rightNull));
        doJoin();
    }

    protected void doJoin() {
        map.forEachLeft((lKey, leftElements) -> {
            List<R> rightElements = map.getRight(lKey);
            if (rightElements != null) {
                consumeMatch(lKey, leftElements, rightElements);
            }
        });
    }

    protected void consumeMatch(K lKey, Stream<L> leftElements, List<R> rightElements) {
        consumer.accept(leftElements, rightElements);
    }
}
