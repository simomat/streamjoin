package de.infonautika.streamjoin;

import de.infonautika.streamjoin.join.JoinWrapper;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class Join {

    public static <L> LeftSide<L> join(Stream<L> left) {
        Objects.requireNonNull(left);
        return new LeftSide<>(left, JoinType.INNER);
    }

    public static <L> LeftSide<L> leftOuter(Stream<L> left) {
        Objects.requireNonNull(left);
        return new LeftSide<>(left, JoinType.LEFT_OUTER);
    }

    public static <L> LeftSide<L> fullOuter(Stream<L> left) {
        Objects.requireNonNull(left);
        return new LeftSide<>(left, JoinType.FULL_OUTER);
    }

    private enum JoinType {
        INNER, LEFT_OUTER, FULL_OUTER
    }

    public static class LeftSide<L> {
        private final Stream<L> left;
        private final JoinType joinType;

        private LeftSide(Stream<L> left, JoinType joinType) {
            this.left = left;
            this.joinType = joinType;
        }

        public <K> LeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            Objects.requireNonNull(leftKeyFunction);
            return new LeftKey<>(leftKeyFunction, this);
        }
    }

    public static class LeftKey<L, K> {

        private final LeftSide<L> leftSide;
        private final Function<L, K> leftKeyFunction;

        private LeftKey(Function<L, K> leftKeyFunction, LeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }

        public <R> RightSide<L, R, K> on(Stream<R> right) {
            Objects.requireNonNull(right);
            return new RightSide<>(right, this);
        }
    }

    public static class RightSide<L, R, K> {
        private final Stream<R> right;
        private final LeftKey<L, K> leftKey;

        private RightSide(Stream<R> right, LeftKey<L, K> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }

        public RightKey<L, R, K> withKey(Function<R, K> rightKeyFunction) {
            Objects.requireNonNull(rightKeyFunction);
            return new RightKey<>(rightKeyFunction, this);
        }
    }

    public static class RightKey<L, R, K> {
        private final Function<R, K> rightKeyFunction;
        private final RightSide<L, R, K> rightSide;

        private RightKey(Function<R, K> rightKeyFunction, RightSide<L, R, K> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> Stream<Y> combine(BiFunction<L, R, Y> combiner) {
            return combine(combiner, null, null);
        }

        public <Y> Stream<Y> combine(BiFunction<L, R, Y> combiner, Function<L, Y> unmatchedLeft) {
            return combine(combiner, unmatchedLeft, null);
        }

        public <Y> Stream<Y> combine(BiFunction<L, R, Y> combiner, Function<L, Y> unmatchedLeft, Function<R, Y> unmatchedRight) {
            Objects.requireNonNull(combiner);

            checkJoinTypeMissmatch(unmatchedLeft, unmatchedRight);

            if (getJoinType().equals(JoinType.LEFT_OUTER) || getJoinType().equals(JoinType.FULL_OUTER)) {
                if (unmatchedLeft == null) {
                    unmatchedLeft = l -> combiner.apply(l, null);
                }
            }

            if (getJoinType().equals(JoinType.FULL_OUTER)) {
                if (unmatchedRight == null) {
                    unmatchedRight = r -> combiner.apply(null, r);
                }
            }


            return groupMany(
                    (l, rs) -> rs.map(r -> combiner.apply(l, r)),
                    unmatchedLeft,
                    unmatchedRight);
        }


        public <Y> Stream<Y> group(BiFunction<L, Stream<R>, Y> grouper) {
            return group(grouper, null, null);
        }

        public <Y> Stream<Y> group(BiFunction<L, Stream<R>, Y> grouper, Function<L, Y> unmatchedLeft) {
            return group(grouper, unmatchedLeft, null);
        }

        public <Y> Stream<Y> group(BiFunction<L, Stream<R>, Y> grouper, Function<L, Y> unmatchedLeft, Function<R, Y> unmatchedRight) {
            Objects.requireNonNull(grouper);

            checkJoinTypeMissmatch(unmatchedLeft, unmatchedRight);

            if (getJoinType().equals(JoinType.LEFT_OUTER) || getJoinType().equals(JoinType.FULL_OUTER)) {
                if (unmatchedLeft == null) {
                    unmatchedLeft = l -> grouper.apply(l, null);
                }
            }

            if (getJoinType().equals(JoinType.FULL_OUTER)) {
                if (unmatchedRight == null) {
                    unmatchedRight = r -> grouper.apply(null, Stream.of(r));
                }
            }

            return groupMany(
                    (left, rightStream) -> Stream.of(grouper.apply(left, rightStream)),
                    unmatchedLeft,
                    unmatchedRight);
        }

        private <Y> void checkJoinTypeMissmatch(Function<L, Y> unmatchedLeft, Function<R, Y> unmatchedRight) {
            if (getJoinType().equals(JoinType.INNER) && (unmatchedLeft != null || unmatchedRight != null)) {
                throw new IllegalArgumentException("no unmatched element handler supported in inner joins");
            }

            if (getJoinType().equals(JoinType.LEFT_OUTER) && unmatchedRight != null) {
                throw new IllegalArgumentException("no unmatched element handler for right elements supported in left outer join.");
            }
        }

        private <Y> Stream<Y> groupMany(BiFunction<L, Stream<R>, Stream<Y>> grouper, Function<L, Y> unmatchedLeft, Function<R, Y> unmatchedRight) {
            Objects.requireNonNull(grouper);

            return JoinWrapper.joinWithParameters(
                    rightSide.leftKey.leftSide.left,
                    rightSide.leftKey.leftKeyFunction,
                    rightSide.right,
                    rightKeyFunction,
                    grouper,
                    unmatchedLeft,
                    unmatchedRight);
        }

        private JoinType getJoinType() {
            return rightSide.leftKey.leftSide.joinType;
        }


    }

}
