package de.infonautika.streamjoin.joins;

import java.util.stream.Stream;

import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;

public class Joiner<Y> {
    private final JoinStrategy<Y> join;
    private Stream<Y> result = empty();

    public Joiner(JoinStrategy<Y> join) {
        this.join = join;
    }

    public Stream<Y> doJoin() {
        join.join(this::addResultPart);
        return result;
    }

    protected void addResultPart(Stream<Y> part) {
        result = concat(result, part);
    }
}
