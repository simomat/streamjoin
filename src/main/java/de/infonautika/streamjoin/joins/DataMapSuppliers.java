package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.joins.indexing.Indexer;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.streamutils.StreamCollector.toStream;
import static java.util.stream.Collectors.toList;

public class DataMapSuppliers {
    public static <L, R, K> Supplier<DataMap<L, R, K>> dataMapOf(Stream<L> left, Function<L, K> leftKey, Stream<R> right, Function<R, K> rightKey) {
        return new Supplier<DataMap<L, R, K>>(){
            private List<R> rightNull;
            private Map<K, List<R>> rightMap;
            private Stream<L> leftNull;
            private Map<K, Stream<L>> leftMap;

            @Override
            public DataMap<L, R, K> get() {
                new Indexer<>(left, leftKey, toStream()).consume((leftMap, leftNull) -> {
                    this.leftMap = leftMap;
                    this.leftNull = leftNull;
                });
                new Indexer<>(right, rightKey, toList()).consume((rightMap, rightNull) -> {
                    this.rightMap = rightMap;
                    this.rightNull = rightNull;
                });
                return new DataMap<>(leftMap, rightMap, leftNull, rightNull);
            }
        };
    }
}
