package de.infonautika.streamjoin.consumer;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class GroupingConsumer<L, R, Y> extends MatchConsumer<L, R, Y> {
    private final BiFunction<L, Stream<R>, Y> grouper;

    public GroupingConsumer(BiFunction<L, Stream<R>, Y> grouper) {
        this.grouper = grouper;
    }

    @Override
    public void accept(Stream<L> leftElements, List<R> rightElements) {
        appendResult(leftElements
                .map(l -> grouper.apply(l, rightElements.stream())));
    }
}
