package de.infonautika.streamjoin;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class StreamMatcher<T> extends TypeSafeMatcher<Stream<T>> {

    private List<T> data;
    private List<T> actualData;
    private Stream<T> actualStream;

    private StreamMatcher(T... data) {
        this.data = asList(data);
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
        mismatchDescription.appendText("stream ");
        if (describeMissing(mismatchDescription)) {
            mismatchDescription.appendText("; ");
        }
        describeAdditional(mismatchDescription);
    }

    private void describeAdditional(Description description) {
        List<T> additional = difference(actualData, data);
        if (additional.size() > 0) {
            description.appendText("contains ");
            describeItems(description, additional);
        }
    }

    private boolean describeMissing(Description description) {
        List<T> missing = difference(data, actualData);
        if (missing.size() > 0) {
            description.appendText("does not contain ");
            describeItems(description, missing);
            return true;
        }
        return false;
    }

    private List<T> difference(List<T> minuend, List<T> subtrahend) {
        ArrayList<T> minu = new ArrayList<>(minuend);
        subtrahend.forEach(minu::remove);
        return minu;
    }

    private void describe(Description description, List<T> streamData) {
        description.appendText("stream of [");
        describeItems(description, streamData);
        description.appendText("]");
    }

    private void describeItems(Description description, List<T> streamData) {
        description.appendText(streamData.stream()
                .map(t -> t == null ? "null" : t.toString())
                .collect(Collectors.joining(", ")));
    }


    public static <T> StreamMatcher<T> isStreamOf(T... data) {
        return new StreamMatcher<>(data);
    }

    public static <T> StreamMatcher<T> isEmptyStream() {
        return isStreamOf();
    }
}
