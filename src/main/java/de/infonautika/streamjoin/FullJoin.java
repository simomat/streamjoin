package de.infonautika.streamjoin;

import de.infonautika.streamjoin.join.Joiner;
import de.infonautika.streamjoin.join.MatchPredicate;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.Combiners.toUnmatchedLeft;
import static de.infonautika.streamjoin.Combiners.toUnmatchedRight;
import static de.infonautika.streamjoin.Guards.checkNotNull;

public class FullJoin {

    public static <L> FJLeftSide<L> fullJoin(Stream<L> left) {
        checkNotNull(left, "left must not be null");
        return new FJLeftSide<>(left);
    }

    public static class FJLeftSide<L> {
        private final Stream<? extends L> left;
        private FJLeftSide(Stream<? extends L> left) {
            this.left = left;
        }
        public <KL> FJLeftKey<L, KL> withKey(Function<? super L, KL> leftKeyFunction) {
            checkNotNull(left, "leftKeyFunction must not be null");
            return new FJLeftKey<>(leftKeyFunction, this);
        }
    }

    public static class FJLeftKey<L, KL> {
        private final FJLeftSide<L> leftSide;
        private final Function<? super L, KL> leftKeyFunction;
        private FJLeftKey(Function<? super L, KL> leftKeyFunction, FJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> FJRightSide<L, R, KL> on(Stream<? extends R> right) {
            checkNotNull(right, "right must not be null");
            return new FJRightSide<>(right, this);
        }
    }

    public static class FJRightSide<L, R, KL> {
        private final Stream<? extends R> right;
        private final FJLeftKey<L, KL> leftKey;
        private FJRightSide(Stream<? extends R> right, FJLeftKey<L, KL> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public <KR> FJRightKey<L, R, KL, KR> withKey(Function<? super R, KR> rightKeyFunction) {
            checkNotNull(rightKeyFunction, "rightKeyFunction must not be null");
            return new FJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class FJRightKey<L, R, KL, KR> {
        private final Function<? super R, KR> rightKeyFunction;
        private final FJRightSide<L, R, KL> rightSide;
        private BiPredicate<KL, KR> matchPredicate = MatchPredicate.equals();

        private FJRightKey(Function<? super R, KR> rightKeyFunction, FJRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> FJApply<L, R, KL, KR, Y> combine(BiFunction<L, R, Y> combiner) {
            checkNotNull(combiner, "combiner must not be null");

            return new FJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction, matchPredicate, combiner, toUnmatchedLeft(combiner), toUnmatchedRight(combiner));
        }

        public FJRightKey<L, R, KL, KR> matching(BiPredicate<KL, KR> matchPredicate) {
            this.matchPredicate = matchPredicate;
            return this;
        }
    }

    public static class FJApply<L, R, KL, KR, Y> {
        private final Stream<? extends L> left;
        private final Function<? super L, KL> leftKeyFunction;
        private final Stream<? extends R> right;
        private final Function<? super R, KR> rightKeyFunction;
        private final BiPredicate<KL, KR> matchPredicate;
        private final BiFunction<? super L, ? super R, Y> combiner;
        private final Function<? super L, Y> unmatchedLeft;
        private final Function<? super R, Y> unmatchedRight;

        private FJApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right, Function<? super R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate, BiFunction<? super L, ? super R, Y> combiner, Function<? super L, Y> unmatchedLeft, Function<? super R, Y> unmatchedRight) {
            this.left = left;
            this.leftKeyFunction = leftKeyFunction;
            this.right = right;
            this.rightKeyFunction = rightKeyFunction;
            this.matchPredicate = matchPredicate;
            this.combiner = combiner;
            this.unmatchedLeft = unmatchedLeft;
            this.unmatchedRight = unmatchedRight;
        }

        public Stream<Y> asStream() {
            return Joiner.fullJoin(
                    left,
                    leftKeyFunction,
                    right,
                    rightKeyFunction,
                    matchPredicate,
                    combiner,
                    unmatchedLeft,
                    unmatchedRight);
        }
    }
}
