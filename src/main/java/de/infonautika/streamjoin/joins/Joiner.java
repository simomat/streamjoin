package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.consumer.MatchConsumer;

import java.util.stream.Stream;

public class Joiner<L, R, Y> {
    private final JoinStrategy<L, R, Y> join;
    private final MatchConsumer<L, R, Y> consumer;

    public Joiner(JoinStrategy<L, R, Y> joinStrategy, MatchConsumer<L, R, Y> consumer) {
        this.join = joinStrategy;
        this.consumer = consumer;
    }

    public Stream<Y> doJoin() {
        join.join(consumer);
        return consumer.getResult();
    }


}
