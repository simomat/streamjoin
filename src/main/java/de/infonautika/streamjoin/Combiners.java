package de.infonautika.streamjoin;

import java.util.function.BiFunction;
import java.util.function.Function;

class Combiners {
    static <L, Y> Function<L, Y> toUnmatchedLeft(BiFunction<? super L, ?, Y> resultHandler) {
        return l -> resultHandler.apply(l, null);
    }

    static <R, Y> Function<R, Y> toUnmatchedRight(BiFunction<?, ? super R, Y> resultHandler) {
        return r -> resultHandler.apply(null, r);
    }
}
