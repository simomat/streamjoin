package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.joins.indexing.DataMap;
import de.infonautika.streamjoin.joins.indexing.Indexer;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class InnerEquiJoin<L, R, K, Y> implements JoinStrategy<Y> {

    private final Indexer<L, R, K> indexer;
    protected final BiFunction<L, Stream<R>, Stream<Y>> grouper;
    private Consumer<Stream<Y>> consumer;
    protected DataMap<L, R, K> map;

    public InnerEquiJoin(Indexer<L, R, K> indexer, BiFunction<L, Stream<R>, Stream<Y>> grouper) {
        this.indexer = indexer;
        this.grouper = grouper;
    }

    @Override
    public void join(Consumer<Stream<Y>> resultConsumer) {
        consumer = resultConsumer;
        map = indexer.getIndexedMap();
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
        addResultPart(leftElements
                .map(l -> grouper.apply(l, rightElements.stream()))
                .flatMap(identity()));
    }

    protected void addResultPart(Stream<Y> resultPart) {
        consumer.accept(resultPart);
    }

}
