package de.infonautika.streamjoin;

import de.infonautika.streamjoin.join.Joiner;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Join {

    public static <L> IJLeftSide<L> join(Stream<L> left) {
        Objects.requireNonNull(left);
        return new IJLeftSide<>(left);
    }

    public static <L> LJLeftSide<L> leftOuter(Stream<L> left) {
        Objects.requireNonNull(left);
        return new LJLeftSide<>(left);
    }

    public static <L> OJLeftSide<L> fullOuter(Stream<L> left) {
        Objects.requireNonNull(left);
        return new OJLeftSide<>(left);
    }

    public static class IJLeftSide<L> {
        private final Stream<L> left;
        private IJLeftSide(Stream<L> left) {
            this.left = left;
        }
        public <K> IJLeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            Objects.requireNonNull(leftKeyFunction);
            return new IJLeftKey<>(leftKeyFunction, this);
        }
    }

    public static class IJLeftKey<L, K> {
        private final IJLeftSide<L> leftSide;
        private final Function<L, K> leftKeyFunction;
        private IJLeftKey(Function<L, K> leftKeyFunction, IJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> IJRightSide<L, R, K> on(Stream<R> right) {
            Objects.requireNonNull(right);
            return new IJRightSide<>(right, this);
        }
    }

    public static class IJRightSide<L, R, K> {
        private final Stream<R> right;
        private final IJLeftKey<L, K> leftKey;
        private IJRightSide(Stream<R> right, IJLeftKey<L, K> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public IJRightKey<L, R, K> withKey(Function<R, K> rightKeyFunction) {
            Objects.requireNonNull(rightKeyFunction);
            return new IJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class IJRightKey<L, R, K> {
        private final Function<R, K> rightKeyFunction;
        private final IJRightSide<L, R, K> rightSide;
        private IJRightKey(Function<R, K> rightKeyFunction, IJRightSide<L, R, K> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> IJApply<L, R, K, Y> combine(BiFunction<L, R, Y> combiner) {
            IJApply<L, R, K, Y> apply = createApply();
            apply.fromCombiner(combiner);
            return apply;
        }

        public <Y> IJApply<L, R, K, Y> group(BiFunction<L, Stream<R>, Y> grouper) {
            IJApply<L, R, K, Y> apply = createApply();
            apply.fromGrouper(grouper);
            return apply;
        }

        private <Y> IJApply<L, R, K, Y> createApply() {
            return new IJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction);
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

    public static class LJLeftKey<L, K> {
        private final LJLeftSide<L> leftSide;
        private final Function<L, K> leftKeyFunction;
        private LJLeftKey(Function<L, K> leftKeyFunction, LJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> LJRightSide<L, R, K> on(Stream<R> right) {
            Objects.requireNonNull(right);
            return new LJRightSide<>(right, this);
        }
    }

    public static class LJRightSide<L, R, K> {
        private final Stream<R> right;
        private final LJLeftKey<L, K> leftKey;
        private LJRightSide(Stream<R> right, LJLeftKey<L, K> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public LJRightKey<L, R, K> withKey(Function<R, K> rightKeyFunction) {
            Objects.requireNonNull(rightKeyFunction);
            return new LJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class LJRightKey<L, R, K> {
        private final Function<R, K> rightKeyFunction;
        private final LJRightSide<L, R, K> rightSide;
        private LJRightKey(Function<R, K> rightKeyFunction, LJRightSide<L, R, K> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> LJApply<L, R, K, Y> combine(BiFunction<L, R, Y> combiner) {
            LJApply<L, R, K, Y> apply = createApply();
            apply.fromCombiner(combiner);
            return apply;
        }

        public <Y> LJApply<L, R, K, Y> group(BiFunction<L, Stream<R>, Y> grouper) {
            LJApply<L, R, K, Y> apply = createApply();
            apply.fromGrouper(grouper);
            return apply;
        }

        private <Y> LJApply<L, R, K, Y> createApply() {
            return new LJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction);
        }
    }

    /////////////////////

    public static class OJLeftSide<L> {
        private final Stream<L> left;
        private OJLeftSide(Stream<L> left) {
            this.left = left;
        }
        public <K> OJLeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            Objects.requireNonNull(leftKeyFunction);
            return new OJLeftKey<>(leftKeyFunction, this);
        }
    }

    public static class OJLeftKey<L, K> {
        private final OJLeftSide<L> leftSide;
        private final Function<L, K> leftKeyFunction;
        private OJLeftKey(Function<L, K> leftKeyFunction, OJLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }
        public <R> OJRightSide<L, R, K> on(Stream<R> right) {
            Objects.requireNonNull(right);
            return new OJRightSide<>(right, this);
        }
    }

    public static class OJRightSide<L, R, K> {
        private final Stream<R> right;
        private final OJLeftKey<L, K> leftKey;
        private OJRightSide(Stream<R> right, OJLeftKey<L, K> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }
        public OJRightKey<L, R, K> withKey(Function<R, K> rightKeyFunction) {
            Objects.requireNonNull(rightKeyFunction);
            return new OJRightKey<>(rightKeyFunction, this);
        }
    }

    public static class OJRightKey<L, R, K> {
        private final Function<R, K> rightKeyFunction;
        private final OJRightSide<L, R, K> rightSide;
        private OJRightKey(Function<R, K> rightKeyFunction, OJRightSide<L, R, K> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> OJApply<L, R, K, Y> combine(BiFunction<L, R, Y> combiner) {
            OJApply<L, R, K, Y> apply = createApply();
            apply.fromCombiner(combiner);
            return apply;
        }

        public <Y> OJApply<L, R, K, Y> group(BiFunction<L, Stream<R>, Y> grouper) {
            OJApply<L, R, K, Y> apply = createApply();
            apply.fromGrouper(grouper);
            return apply;
        }

        private <Y> OJApply<L, R, K, Y> createApply() {
            return new OJApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction, rightSide.right, rightKeyFunction);
        }
    }

    ////////////////

    public static class IJApply<L, R, K, Y> {
        protected BiFunction<L, R, Y> combiner = null;
        protected BiFunction<L, Stream<R>, Y> grouper = null;

        protected Function<L, Y> unmatchedLeft;
        protected Function<R, Y> unmatchedRight;

        private Stream<L> left;
        private Function<L, K> leftKeyFunction;
        private Stream<R> right;
        private Function<R, K> rightKeyFunction;

        public IJApply(Stream<L> left, Function<L, K> leftKeyFunction, Stream<R> right, Function<R, K> rightKeyFunction) {
            this.left = left;
            this.leftKeyFunction = leftKeyFunction;
            this.right = right;
            this.rightKeyFunction = rightKeyFunction;
        }

        public Stream<Y> asStream() {
            return resultStream();
        }

        public <C> C collect(Collector<Y, ?, C> collector) {
            return resultStream().collect(collector);
        }

        private Stream<Y> resultStream() {
            if ((grouper == null && combiner == null) || (grouper != null && combiner != null)) {
                throw new IllegalStateException();
            }

            Stream<Y> result;
            if (grouper != null) {
                result = doJoin((left, rightStream) -> Stream.of(grouper.apply(left, rightStream)));
            } else {
                result = doJoin((l, rs) -> rs.map(r -> combiner.apply(l, r)));
            }
            return result;
        }

        protected Stream<Y> doJoin(BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            return Joiner.join(
                    left,
                    leftKeyFunction,
                    right,
                    rightKeyFunction,
                    groupMany,
                    unmatchedLeft,
                    unmatchedRight);
        }

        void fromGrouper(BiFunction<L, Stream<R>, Y> grouper) {
            this.grouper = grouper;
        }

        public void fromCombiner(BiFunction<L, R, Y> combiner) {
            this.combiner = combiner;
        }
    }

    public static class LJApply<L, R, K, Y> extends IJApply<L, R, K, Y> {
        public LJApply(Stream<L> left, Function<L, K> leftKeyFunction, Stream<R> right, Function<R, K> rightKeyFunction) {
            super(left, leftKeyFunction, right, rightKeyFunction);
            unmatchedLeft = l -> combiner.apply(l, null);
        }

        public LJApply<L, R, K, Y> withLeftUnmatched(Function<L, Y> unmatchedLeft) {
            this.unmatchedLeft = unmatchedLeft;
            return this;
        }

        @Override
        void fromGrouper(BiFunction<L, Stream<R>, Y> grouper) {
            super.fromGrouper(grouper);
            unmatchedLeft = l -> grouper.apply(l, null);
        }

        @Override
        public void fromCombiner(BiFunction<L, R, Y> combiner) {
            super.fromCombiner(combiner);
            unmatchedLeft = l -> combiner.apply(l, null);
        }
    }

    public static class OJApply<L, R, K, Y> extends LJApply<L, R, K, Y> {
        public OJApply(Stream<L> left, Function<L, K> leftKeyFunction, Stream<R> right, Function<R, K> rightKeyFunction) {
            super(left, leftKeyFunction, right, rightKeyFunction);
            unmatchedRight = r -> grouper.apply(null, Stream.of(r));
        }

        public OJApply<L, R, K, Y> withRightUnmatched(Function<R, Y> unmatchedRight) {
            this.unmatchedRight = unmatchedRight;
            return this;
        }

        @Override
        void fromGrouper(BiFunction<L, Stream<R>, Y> grouper) {
            super.fromGrouper(grouper);
            unmatchedRight = r -> grouper.apply(null, Stream.of(r));
        }


        @Override
        public void fromCombiner(BiFunction<L, R, Y> combiner) {
            super.fromCombiner(combiner);
            unmatchedRight = r -> combiner.apply(null, r);
        }
    }
}
