package de.infonautika.streamjoin.joins.util;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class StreamCollectorTest {
    @Test
    public void supplierReturnsBuilder() throws Exception {
        Collector<Integer, CombiningStreamBuilder<Integer>, Stream<Integer>> collector = new StreamCollector<>();
        CombiningStreamBuilder<Integer> builder = collector.supplier().get();

        assertThat(builder, is(instanceOf(CombiningStreamBuilder.class)));
    }

    @Test
    public void accumulatorAccumulates() throws Exception {
        Collector<Integer, CombiningStreamBuilder<Integer>, Stream<Integer>> collector = new StreamCollector<>();
        CombiningStreamBuilder<Integer> builder = new CombiningStreamBuilder<>();

        collector.accumulator().accept(builder, 2);
        List<Integer> collected = builder.build().collect(toList());

        assertThat(collected, contains(2));
    }

    @Test
    public void combinerCombines() throws Exception {
        CombiningStreamBuilder<Integer> left = new CombiningStreamBuilder<>();
        CombiningStreamBuilder<Integer> right = new CombiningStreamBuilder<>();
        left.accept(1);
        right.accept(2);

        List<Integer> collected = new StreamCollector<Integer>()
                .combiner()
                .apply(left, right)
                .build()
                .collect(toList());

        assertThat(collected, containsInAnyOrder(1, 2));
    }

    @Test
    public void finisherFinishes() throws Exception {
        CombiningStreamBuilder<Integer> builder = new CombiningStreamBuilder<>();
        builder.accept(2);

        List<Integer> collected = new StreamCollector<Integer>()
                .finisher()
                .apply(builder)
                .collect(toList());

        assertThat(collected, contains(2));
    }

}