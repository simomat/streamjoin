package de.infonautika.streamjoin;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.streamutils.StreamCollector.toStream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class FunctionalJoin {
    public static <Y, L, R, K> Stream<Y> joinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Stream<R> right,
            Function<R, K> rightKeyFunction,
            BiFunction<L, Stream<R>, Y> grouper) {

        Stream.Builder<Stream<Y>> builder = Stream.builder();
        Consumer<Stream<Y>> partConsumer = builder;
        join(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                (leftElements, rightElements) ->
                        partConsumer.accept(
                            leftElements
                                .map(l -> grouper.apply(l, rightElements.stream()))));

        return builder.build().flatMap(identity());
    }

    private static <L, R, K> void join(Stream<L> left, Function<L, K> leftKeyFunction, Stream<R> right, Function<R, K> rightKeyFunction, BiConsumer<Stream<L>, List<R>> consumer) {
        joinWithGrouper(
                left,
                leftKeyFunction,
                getMap(right, rightKeyFunction, toList())::get,
                consumer);
    }

    private static <L, K, R> void joinWithGrouper(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Function<K, List<R>> rightIndexed,
            BiConsumer<Stream<L>, List<R>> matcher) {

        getMap(left, leftKeyFunction, toStream())
                .forEach((key, leftDownstream) ->
                        Optional.ofNullable(rightIndexed.apply(key))
                                .ifPresent(rightDownstream -> matcher.accept(leftDownstream, rightDownstream)));
    }

    private static <T, K, D> Map<K, D> getMap(Stream<T> elements, Function<T, K> classifier, Collector<T, ?, D> downstream) {
        Map<K, D> map = elements.collect(
                groupingBy(
                        nullTolerantClassifier(classifier),
                        downstream));

        map.remove(nullKey());
        return map;
    }

    private static <T, K> Function<? super T, ? extends K> nullTolerantClassifier(Function<T, K> classifier) {
        return element -> Optional.ofNullable(classifier.apply(element)).orElse(nullKey());
    }

    private static final Object NULLKEY =new Object();


    private static <K> K nullKey() {
        return (K) NULLKEY;
    }


}
