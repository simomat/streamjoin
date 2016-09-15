package de.infonautika.streamjoin.joins.indexing;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

public class IndexerTest {

    @Test
    public void emptyStreamYieldsResult() throws Exception {
        Indexer<Object, Object, List<Object>> indexer = new Indexer<>(Stream.empty(), identity(), toList());

        @SuppressWarnings("unchecked")
        BiConsumer<Map<Object, List<Object>>, List<Object>> consumer = (BiConsumer<Map<Object, List<Object>>, List<Object>>)mock(BiConsumer.class);
        indexer.consume(consumer);

        verify(consumer, times(1)).accept(
                argThat((map) -> map.size() == 0),
                isNull());
    }

    @Test
    public void itemsAreIndexed() throws Exception {
        Indexer<Integer, Integer, List<Integer>> indexer = new Indexer<>(Stream.of(1, 2, 2), identity(), toList());

        @SuppressWarnings("unchecked")
        BiConsumer<Map<Integer, List<Integer>>, List<Integer>> consumer = (BiConsumer<Map<Integer, List<Integer>>, List<Integer>>)mock(BiConsumer.class);

        indexer.consume(consumer);
        verify(consumer, times(1)).accept(
                argThat((map) ->
                        map.size() == 2
                        && hasEntry(map, 1, asList(1))
                        && hasEntry(map, 2, asList(2, 2))),
                isNull());
    }

    @Test
    public void nullKeyElementsGoToNullMap() throws Exception {
        Indexer<Integer, Integer, List<Integer>> indexer = new Indexer<>(Stream.of(1, 2, 2), (x) -> null, toList());

        @SuppressWarnings("unchecked")
        BiConsumer<Map<Integer, List<Integer>>, List<Integer>> consumer = (BiConsumer<Map<Integer, List<Integer>>, List<Integer>>)mock(BiConsumer.class);

        indexer.consume(consumer);
        verify(consumer, times(1)).accept(
                argThat((map) -> map.size() == 0),
                argThat((list) -> hasItemsInAnyOrder(list, 1, 2, 2)));
    }

    @SafeVarargs
    private final <T> boolean hasItemsInAnyOrder(List<T> list, T... entries) {
        List<T> expected = new ArrayList<>(asList(entries));
        return expected.size() == list.size()
                && list.stream()
                    .allMatch(expected::remove);
    }

    private <K, T> boolean hasEntry(Map<K, List<T>> map, K key, List<T> value) {
        List<T> items = map.get(key);
        return items != null && items.equals(value);
    }

}