package de.infonautika.streamjoin.streamutils;

import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CombiningStreamBuilderTest {
    @Test
    public void emptyBuilderBuildsEmptyStream() throws Exception {
        List<Integer> collected = new CombiningStreamBuilder<Integer>().build().collect(toList());

        assertThat(collected, is(empty()));
    }

    @Test
    public void addingItemsBuildsItemStream() throws Exception {
        List<Integer> collected = new CombiningStreamBuilder<Integer>()
                .add(1)
                .add(2)
                .build()
                .collect(toList());

        assertThat(collected, containsInAnyOrder(1, 2));
    }

    @Test
    public void combineEmptyBuilderKeepsResult() throws Exception {
        CombiningStreamBuilder<Integer> builder = new CombiningStreamBuilder<>();

        builder.add(1).add(2);
        builder.combine(Stream.builder());
        List<Integer> collected = builder.build().collect(toList());

        assertThat(collected, containsInAnyOrder(1, 2));
    }

    @Test
    public void combineBuilderCombinesStream() throws Exception {
        CombiningStreamBuilder<Integer> builder = new CombiningStreamBuilder<>();
        builder.add(1).add(2);

        List<Integer> collected = builder
                .combine(
                        Stream.<Integer>builder().add(3).add(4))
                .build()
                .collect(toList());

        assertThat(collected, containsInAnyOrder(1, 2, 3, 4));
    }

    @Test
    public void usingOBuildersAfterCombineYieldsAll() throws Exception {
        Stream.Builder<Integer> builder1 = Stream.<Integer>builder().add(1);
        CombiningStreamBuilder<Integer> builder2 = new CombiningStreamBuilder<>();
        builder2.add(2);

        builder2.combine(builder1);

        builder1.accept(3);
        builder2.accept(4);

        List<Integer> collected = builder2
                .build()
                .collect(toList());

        assertThat(collected, containsInAnyOrder(1, 2, 3, 4));

    }
}