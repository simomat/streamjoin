package de.infonautika.streamjoin.consumer;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class CombiningConsumer<L, R, Y> extends MatchConsumer<L, R, Y> {
    private final BiFunction<L, R, Y> combiner;

    public CombiningConsumer(BiFunction<L, R, Y> combiner) {
        this.combiner = combiner;
    }

    @Override
    public void accept(Stream<L> leftElements, List<R> rightElements) {
        leftElements.forEach(l ->
                appendResult(rightElements.stream().map(r -> combiner.apply(l, r))));
    }
}
