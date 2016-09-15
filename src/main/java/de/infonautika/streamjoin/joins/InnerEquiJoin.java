package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.consumer.MatchConsumer;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class InnerEquiJoin<L, R, K, Y> implements JoinStrategy<L, R, Y> {

    protected MatchConsumer<L, R, Y> consumer;
    protected DataMap<L, R, K> map;
    private Supplier<DataMap<L, R, K>> dataMapSupplier;

    public InnerEquiJoin(Supplier<DataMap<L, R, K>> dataMapSupplier) {
        this.dataMapSupplier = dataMapSupplier;
    }

    @Override
    public void join(MatchConsumer<L, R, Y> consumer) {
        this.consumer = consumer;
        map = dataMapSupplier.get();
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
