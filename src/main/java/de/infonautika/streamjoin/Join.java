package de.infonautika.streamjoin;

import de.infonautika.streamjoin.join.Joiner;
import de.infonautika.streamjoin.join.MatchPredicate;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

public class Join {

    public static <L> IJLeftSide<L> join(Stream<? extends L> left) {
        checkNotNull(left, "left must not be null");
        return new IJLeftSide<>(left);
    }

    public static <L> LJLeftSide<L> leftOuter(Stream<L> left) {
        checkNotNull(left, "left must not be null");
        return new LJLeftSide<>(left);
    }

    public static class IJLeftSide<L> {
        private final Stream<? extends L> left;
        private IJLeftSide(Stream<? extends L> left) {
            this.left = left;
        }
        public <KL> IJLeftKey<L, KL> withKey(Function<? super L, KL> leftKeyFunction) {
            checkNotNull(left, "leftKeyFunction must not be null");
            return new IJLeftKey<>(leftKeyFunction, this);
        }
    }

    public static class IJLeftKey<L, KL> {
        private final IJLeftSide<L> leftSide;
        private final Function<? super L, KL> leftKeyFunction;
        private IJLeftKey(Function<? super L, KL> leftKeyFunction, IJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> IJRightSide<L, R, KL> on(Stream<? extends R> right) {
            checkNotNull(right, "right must not be null");
            return new IJRightSide<>(right, this);
        }
    }

    public static class IJRightSide<L, R, KL> {
        private final Stream<? extends R> right;
        private final IJLeftKey<L, KL> leftKey;
        private IJRightSide(Stream<? extends R> right, IJLeftKey<L, KL> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public <KR> IJRightKey<L, R, KL, KR> withKey(Function<? super R, KR> rightKeyFunction) {
            checkNotNull(rightKeyFunction, "rightKeyFunction must not be null");
            return new IJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class IJRightKey<L, R, KL, KR> {
        private final Function<? super R, KR> rightKeyFunction;
        private final IJRightSide<L, R, KL> rightSide;
        private BiPredicate<KL, KR> matchPredicate = MatchPredicate.equals();

        private IJRightKey(Function<? super R, KR> rightKeyFunction, IJRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> IJApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            checkNotNull(combiner, "combiner must not be null");
            return createApplyWithCombiner(combinerToGroupMany(combiner));
        }

        public <Y> IJApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            checkNotNull(grouper, "grouper must not be null");
            return createApplyWithCombiner(grouperToGroupMany(grouper));
        }

        private <Y> IJApply<L, R, KL, KR, Y> createApplyWithCombiner(BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            return new IJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction, matchPredicate, groupMany);
        }

        public IJRightKey<L, R, KL, KR> matching(BiPredicate<KL, KR> matchPredicate) {
            this.matchPredicate = matchPredicate;
            return this;
        }
    }

    public static class LJLeftSide<L> {
        private final Stream<L> left;
        private LJLeftSide(Stream<L> left) {
            this.left = left;
        }
        public <K> LJLeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            checkNotNull(leftKeyFunction, "leftKeyFunction must not be null");
            return new LJLeftKey<>(leftKeyFunction, this);
        }
    }

    public static class LJLeftKey<L, KL> {
        private final LJLeftSide<L> leftSide;
        private final Function<L, KL> leftKeyFunction;
        private LJLeftKey(Function<L, KL> leftKeyFunction, LJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> LJRightSide<L, R, KL> on(Stream<R> right) {
            checkNotNull(right, "right must not be null");
            return new LJRightSide<>(right, this);
        }
    }

    public static class LJRightSide<L, R, KL> {
        private final Stream<R> right;
        private final LJLeftKey<L, KL> leftKey;
        private LJRightSide(Stream<R> right, LJLeftKey<L, KL> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public <KR> LJRightKey<L, R, KL, KR> withKey(Function<R, KR> rightKeyFunction) {
            checkNotNull(rightKeyFunction, "rightKeyFunction must not be null");
            return new LJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class LJRightKey<L, R, KL, KR> {
        private final Function<R, KR> rightKeyFunction;
        private final LJRightSide<L, R, KL> rightSide;
        private BiPredicate<KL, KR> matchPredicate = MatchPredicate.equals();

        private LJRightKey(Function<R, KR> rightKeyFunction, LJRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> LJApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            checkNotNull(combiner, "combiner must not be null");
            return createApplyWithCombiner(combinerToGroupMany(combiner), toUnmatchedLeft(combiner));
        }

        public <Y> LJApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            checkNotNull(grouper, "grouper must not be null");
            return createApplyWithCombiner(grouperToGroupMany(grouper), toUnmatchedLeft(grouper));
        }

        public LJRightKey<L, R, KL, KR> matching(BiPredicate<KL, KR> matchPredicate) {
            this.matchPredicate = matchPredicate;
            return this;
        }

        private <Y> LJApply<L, R, KL, KR, Y> createApplyWithCombiner(BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            return new LJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction, matchPredicate, groupMany, unmatchedLeft);
        }
    }

    public static class IJApply<L, R, KL, KR, Y> {
        private final Stream<? extends L> left;
        private final Function<? super L, KL> leftKeyFunction;
        private final Stream<? extends R> right;
        private final Function<? super R, KR> rightKeyFunction;
        private final BiPredicate<KL, KR> matchPredicate;
        private BiFunction<L, Stream<R>, Stream<Y>> groupMany;
        Function<? super L, ? extends Y> unmatchedLeft;

        private IJApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right, Function<? super R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate, BiFunction<L, Stream<R>, Stream<Y>>groupMany) {
            this(left, leftKeyFunction, right, rightKeyFunction, matchPredicate, groupMany, null);
        }

        IJApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right, Function<? super R, KR> rightKeyFunction,  BiPredicate<KL, KR> matchPredicate, BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            this.left = left;
            this.leftKeyFunction = leftKeyFunction;
            this.right = right;
            this.rightKeyFunction = rightKeyFunction;
            this.matchPredicate = matchPredicate;
            this.groupMany = groupMany;
            this.unmatchedLeft = unmatchedLeft;
        }

        public Stream<Y> asStream() {
            return Joiner.join(
                    left,
                    leftKeyFunction,
                    right,
                    rightKeyFunction,
                    matchPredicate,
                    groupMany,
                    unmatchedLeft);
        }
    }

    public static class LJApply<L, R, KL, KR, Y> extends IJApply<L, R, KL, KR, Y> {

        private LJApply(Stream<L> left, Function<L, KL> leftKeyFunction, Stream<R> right, Function<R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate, BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            super(left, leftKeyFunction, right, rightKeyFunction, matchPredicate, groupMany, unmatchedLeft);
        }

        public LJApply<L, R, KL, KR, Y> withLeftUnmatched(Function<? super L, ? extends Y> unmatchedLeft) {
            checkNotNull(unmatchedLeft, "unmatchedLeft must not be null");
            this.unmatchedLeft = unmatchedLeft;
            return this;
        }
    }

    private static <L, R, Y> BiFunction<L, Stream<R>, Stream<Y>> combinerToGroupMany(BiFunction<? super L, ? super R, Y> combiner) {
        return (l, rs) -> rs.map(r -> combiner.apply(l, r));
    }

    private static <L, R, Y> BiFunction<L, Stream<R>, Stream<Y>> grouperToGroupMany(BiFunction<? super L, Stream<R>, Y> grouper) {
        return (left, rightStream) -> Stream.of(grouper.apply(left, rightStream));
    }

    private static <L, Y> Function<L, Y> toUnmatchedLeft(BiFunction<? super L, ?, Y> resultHandler) {
        return l -> resultHandler.apply(l, null);
    }

    private static void checkNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
