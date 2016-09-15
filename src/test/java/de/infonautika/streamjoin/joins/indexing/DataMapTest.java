package de.infonautika.streamjoin.joins.indexing;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.joins.indexing.StreamMatcher.isEmptyStream;
import static de.infonautika.streamjoin.joins.indexing.StreamMatcher.isStreamOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataMapTest {

    @Test
    public void getLeftReturnsNullOfMissing() throws Exception {
        DataMap<Integer, ?, Integer> dataMap = new DataMap<>(new HashMap<>(), null, null, null);
        assertThat(dataMap.getLeft(1), is(nullValue()));
    }

    @Test
    public void getRightReturnsNullOfMissing() throws Exception {
        DataMap<?, Integer, Integer> dataMap = new DataMap<>(null, new HashMap<>(), null, null);
        assertThat(dataMap.getRight(1), is(nullValue()));
    }

    @Test
    public void getLeftReturnsCorrectItem() throws Exception {
        HashMap<Integer, Stream<Integer>> left = new HashMap<>();
        List<Integer> leftOneData = asList(1, 2);
        left.put(1, leftOneData.stream());
        left.put(2, Stream.of(2));

        DataMap<Integer, ?, Integer> dataMap = new DataMap<>(left, null, null, null);
        assertThat(dataMap.getLeft(1), isStreamOf(leftOneData));
    }

    @Test
    public void getRightReturnsCorrectItem() throws Exception {
        HashMap<Integer, List<Integer>> right = new HashMap<>();
        List<Integer> list = asList(1, 2);
        right.put(1, list);
        right.put(2, emptyList());

        DataMap<?, ?, Integer> dataMap = new DataMap<>(null, right, null, null);
        assertThat(dataMap.getRight(1), is(list));
    }

    @Test
    public void forEachLeftIsCalledForEachLeft() throws Exception {
        HashMap<Integer, Stream<Integer>> left = new HashMap<>();
        Stream<Integer> stream1 = Stream.of(1, 2);
        Stream<Integer> stream2 = Stream.of(3, 4);
        left.put(1, stream1);
        left.put(2, stream2);

        DataMap<Integer, ?, Integer> dataMap = new DataMap<>(left, null, null, null);


        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Stream<Integer>> biConsumerMock = (BiConsumer<Integer, Stream<Integer>>)mock(BiConsumer.class);

        dataMap.forEachLeft(biConsumerMock);

        // cannot argThat(isStreamOf(...)) because the stream is closed inside of mockito before passed to the matcher :(
        // so here we introduce object identity of DataMap.forEach() :((
        verify(biConsumerMock, times(1)).accept(eq(1), same(stream1));
        verify(biConsumerMock, times(1)).accept(eq(2), same(stream2));
        verify(biConsumerMock, times(2)).accept(any(), any());
    }

    @Test
    public void leftNullElementsIsEmptyStreamWhenPassingNull() throws Exception {
        DataMap<?, ?, ?> dataMap = new DataMap<>(null, null, null, null);

        Stream<?> leftNullKeyElements = dataMap.getLeftNullKeyElements();

        assertThat(leftNullKeyElements, isEmptyStream());
    }

    @Test
    public void rightNullElementsIsEmptyStreamWhenPassingNull() throws Exception {
        DataMap<?, ?, ?> dataMap = new DataMap<>(null, null, null, null);

        List<?> rightNullKeyElements = dataMap.getRightNullKeyElements();

        assertThat(rightNullKeyElements, equalTo(emptyList()));
    }

    @Test
    public void leftNullElementsPassesCorrectElements() throws Exception {
        List<Integer> streamData = asList(1, 2, 3);
        Stream<Integer> stream = streamData.stream();
        DataMap<Integer, ?, ?> dataMap = new DataMap<>(null, null, stream, null);

        assertThat(dataMap.getLeftNullKeyElements(), isStreamOf(streamData));
    }

    @Test
    public void rightNullElementsPassesCorrectElements() throws Exception {
        List<Integer> list = asList(1, 2, 3);
        DataMap<?, Integer, ?> dataMap = new DataMap<>(null, null, null, list);

        assertThat(dataMap.getRightNullKeyElements(), equalTo(list));
    }
}