package de.infonautika.streamjoin.joins.indexing;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class StreamMatcher<T> extends TypeSafeMatcher<Stream<T>> {

    private List<T> data;
    private List<T> actualData;
    private Stream<T> actualStream;

    public StreamMatcher(List<T> data) {
        this.data = data;
    }

    @Override
    protected boolean matchesSafely(Stream<T> stream) {
        actualData = stream.collect(toList());
        actualStream = stream;
        return isDataInAnyOrder();
    }

    private boolean isDataInAnyOrder() {
        if (data.size() != actualData.size()) {
            return false;
        }

        List<T> copyOfData = new ArrayList<>(data);
        return actualData.stream()
                .allMatch(copyOfData::remove);
    }

    @Override
    public void describeTo(Description description) {
        describe(description, this.data);
    }

    @Override
    protected void describeMismatchSafely(Stream<T> item, Description mismatchDescription) {
        assert item == actualStream;
        describe(mismatchDescription, actualData);
    }

    private void describe(Description description, List<T> streamData) {
        description.appendText("a stream of [");
        description.appendText(streamData.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ")));
        description.appendText("]");
    }


    public static <T> StreamMatcher<T> isStreamOf(List<T> data) {
        return new StreamMatcher<>(data);
    }

    public static <T> StreamMatcher<T> isEmptyStream() {
        return isStreamOf(emptyList());
    }
}
