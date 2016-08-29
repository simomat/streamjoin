package de.infonautika.streamjoin.joins;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface JoinStrategy<Y> {
    void join(Consumer<Stream<Y>> resultConsumer);
}
