package de.infonautika.streamjoin.consumer;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Stream.concat;

public abstract class MatchConsumer<L, R, Y> {

    private Stream<Y> result = Stream.empty();

    public abstract void accept(Stream<L> leftElements, List<R> rightElements);

    protected void appendResult(Stream<Y> stream) {
        result = concat(result, stream);
    }

    public Stream<Y> getResult() {
        return result;
    }
}
