package de.infonautika.streamjoin;

import de.infonautika.streamjoin.consumer.CombiningConsumer;
import de.infonautika.streamjoin.consumer.GroupingConsumer;
import de.infonautika.streamjoin.consumer.MatchConsumer;
import de.infonautika.streamjoin.joins.*;
import de.infonautika.streamjoin.joins.indexing.Indexer;

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
            Objects.requireNonNull(combiner);
            return createJoiner(new CombiningConsumer<>(combiner))
                    .doJoin();
        }

        public <Y> Stream<Y> group(BiFunction<L, Stream<R>, Y> grouper) {
            Objects.requireNonNull(grouper);
            return createJoiner(new GroupingConsumer<>(grouper))
                    .doJoin();
        }

        private <Y> Joiner<L, R, Y> createJoiner(MatchConsumer<L, R, Y> consumer) {
            return new Joiner<>(
                    createJoinStrategy(
                            getIndexer(),
                            getJoinType()),
                    consumer);
        }

        private Indexer<L, R, K> getIndexer() {
            return new Indexer<>(
                    rightSide.leftKey.leftSide.left,
                    rightSide.leftKey.leftKeyFunction,
                    rightSide.right,
                    rightKeyFunction);
        }

        private JoinType getJoinType() {
            return rightSide.leftKey.leftSide.joinType;
        }

        private <Y> JoinStrategy<L, R, Y> createJoinStrategy(Indexer<L, R, K> indexer, JoinType joinType) {
            if (joinType.equals(JoinType.INNER)) {
                return new InnerEquiJoin<>(indexer);
            }

            if (joinType.equals(JoinType.LEFT_OUTER)) {
                return new LeftOuterJoin<>(indexer);
            }

            if (joinType.equals(JoinType.FULL_OUTER)) {
                return new FullOuterJoin<>(indexer);
            }

            throw new UnsupportedOperationException();
        }
    }

}
