package de.infonautika.streamjoin.joins.util;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class StreamCollector<T> implements Collector<T, CombiningStreamBuilder<T>, Stream<T>> {

    @Override
    public Supplier<CombiningStreamBuilder<T>> supplier() {
        return CombiningStreamBuilder::new;
    }

    @Override
    public BiConsumer<CombiningStreamBuilder<T>, T> accumulator() {
        return CombiningStreamBuilder::accept;
    }

    @Override
    public BinaryOperator<CombiningStreamBuilder<T>> combiner() {
        return CombiningStreamBuilder::combine;
    }

    @Override
    public Function<CombiningStreamBuilder<T>, Stream<T>> finisher() {
        return CombiningStreamBuilder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }

}
