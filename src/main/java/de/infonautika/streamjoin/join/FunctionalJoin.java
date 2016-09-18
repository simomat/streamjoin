package de.infonautika.streamjoin.join;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class FunctionalJoin {
    static <L, R, K> void join(
            Stream<L> left,
            Function<L, K> leftKeyFunction,
            Clustered<R, K> rightCluster,
            BiConsumer<L, Stream<R>> grouper,
            Consumer<L> unmatchedLeftConsumer,
            Consumer<R> unmatchedRightConsumer) {

        Function<L, K> leftUnmatched = l -> {
            unmatchedLeftConsumer.accept(l);
            return null;
        };

        Set<K> matchedKeys = left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                        .map(key -> rightCluster.getCluster(key)
                                .map((StreamToFunction<R, L, K>) cluster -> left1 -> {
                                    grouper.accept(left1, cluster);
                                    return key;
                                })
                                .orElse(leftUnmatched))
                        .orElse(leftUnmatched)
                        .apply(leftElement))
                .filter(matchedKey -> matchedKey != null)
                .collect(Collectors.toSet());

        rightCluster.getMisfits(matchedKeys)
                .forEach(unmatchedRightConsumer);

    }

    private interface StreamToFunction<R, L, K> extends Function<Stream<R>, Function<L, K>> {}
}
