package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.consumer.MatchConsumer;

public interface JoinStrategy<L, R, Y> {
    void join(MatchConsumer<L, R, Y> consumer);
}
