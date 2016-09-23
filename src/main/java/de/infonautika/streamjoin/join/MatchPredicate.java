package de.infonautika.streamjoin.join;

import java.util.function.BiPredicate;

public class MatchPredicate {
    static final BiPredicate<Object, Object> EQUALS = Object::equals;

    public static <KL, KR> BiPredicate<KL, KR> equals() {
        //noinspection unchecked
        return (BiPredicate<KL, KR>)EQUALS;
    }
}
