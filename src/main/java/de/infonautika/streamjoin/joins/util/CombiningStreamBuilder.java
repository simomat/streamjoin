package de.infonautika.streamjoin.joins.util;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class CombiningStreamBuilder<T> implements Stream.Builder<T> {
    private Stream.Builder<T> current;
    private final List<Stream.Builder<T>> builders;

    public CombiningStreamBuilder() {
        current = Stream.builder();
        builders = new LinkedList<>();
        builders.add(current);
    }

    @Override
    public void accept(T t) {
        current.add(t);
    }

    @Override
    public Stream<T> build() {
        return builders.stream()
                .map(Stream.Builder::build)
                .flatMap(identity());
    }

    public CombiningStreamBuilder<T> combine(Stream.Builder<T> next) {
        current = next;
        builders.add(next);
        return this;
    }
}
