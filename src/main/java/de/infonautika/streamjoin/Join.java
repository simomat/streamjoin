package de.infonautika.streamjoin;

import de.infonautika.streamjoin.join.Joiner;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Join {

    public static <L> IJLeftSide<L> join(Stream<? extends L> left) {
        Objects.requireNonNull(left);
        return new IJLeftSide<>(left);
    }

    public static <L> LJLeftSide<L> leftOuter(Stream<L> left) {
        Objects.requireNonNull(left);
        return new LJLeftSide<>(left);
    }

    public static class IJLeftSide<L> {
        private final Stream<? extends L> left;
        private IJLeftSide(Stream<? extends L> left) {
            this.left = left;
        }
        public <KL> IJLeftKey<L, KL> withKey(Function<? super L, KL> leftKeyFunction) {
            Objects.requireNonNull(leftKeyFunction);
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
            Objects.requireNonNull(right);
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
            Objects.requireNonNull(rightKeyFunction);
            return new IJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class IJRightKey<L, R, KL, KR> {
        private final Function<? super R, KR> rightKeyFunction;
        private final IJRightSide<L, R, KL> rightSide;
        private IJRightKey(Function<? super R, KR> rightKeyFunction, IJRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> IJApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            return createApplyWithCombiner(combinerToGroupMany(combiner));
        }

        public <Y> IJApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            return createApplyWithCombiner(grouperToGroupMany(grouper));
        }

        private <Y> IJApply<L, R, KL, KR, Y> createApplyWithCombiner(BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            return new IJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction, groupMany);
        }
    }

    ///////////////////

    public static class LJLeftSide<L> {
        private final Stream<L> left;
        private LJLeftSide(Stream<L> left) {
            this.left = left;
        }
        public <K> LJLeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            Objects.requireNonNull(leftKeyFunction);
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
            Objects.requireNonNull(right);
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
            Objects.requireNonNull(rightKeyFunction);
            return new LJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class LJRightKey<L, R, KL, KR> {
        private final Function<R, KR> rightKeyFunction;
        private final LJRightSide<L, R, KL> rightSide;
        private LJRightKey(Function<R, KR> rightKeyFunction, LJRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> LJApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            return createApplyWithCombiner(combinerToGroupMany(combiner), toUnmatchedLeft(combiner));
        }

        public <Y> LJApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            return createApplyWithCombiner(grouperToGroupMany(grouper), toUnmatchedLeft(grouper));
        }

        private <Y> LJApply<L, R, KL, KR, Y> createApplyWithCombiner(BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            return new LJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction, groupMany, unmatchedLeft);
        }
    }

    ////////////////

    public static class IJApply<L, R, KL, KR, Y> {
        private final Stream<? extends L> left;
        private final Function<? super L, KL> leftKeyFunction;
        private final Stream<? extends R> right;
        private final Function<? super R, KR> rightKeyFunction;
        private BiFunction<L, Stream<R>, Stream<Y>> groupMany;
        protected Function<? super L, ? extends Y> unmatchedLeft;

        public IJApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right, Function<? super R, KR> rightKeyFunction, BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            this(left, leftKeyFunction, right, rightKeyFunction, groupMany, null);
        }

        public IJApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right, Function<? super R, KR> rightKeyFunction, BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            this.left = left;
            this.leftKeyFunction = leftKeyFunction;
            this.right = right;
            this.rightKeyFunction = rightKeyFunction;
            this.groupMany = groupMany;
            this.unmatchedLeft = unmatchedLeft;
        }

        public Stream<Y> asStream() {
            return resultStream();
        }

        public <C> C collect(Collector<Y, ?, C> collector) {
            return resultStream().collect(collector);
        }

        private Stream<Y> resultStream() {
            return Joiner.join(
                    left,
                    leftKeyFunction,
                    right,
                    rightKeyFunction,
                    groupMany,
                    unmatchedLeft);
        }
    }

    public static class LJApply<L, R, KL, KR, Y> extends IJApply<L, R, KL, KR, Y> {

        public LJApply(Stream<L> left, Function<L, KL> leftKeyFunction, Stream<R> right, Function<R, KR> rightKeyFunction, BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            super(left, leftKeyFunction, right, rightKeyFunction, groupMany, unmatchedLeft);
        }

        public LJApply<L, R, KL, KR, Y> withLeftUnmatched(Function<? super L, ? extends Y> unmatchedLeft) {
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

}
