package de.infonautika.streamjoin.join;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.StreamMatcher.isStreamOf;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ClusterTest {

    @Test
    public void matchNothingByEquality() throws Exception {
        Cluster<Integer, Integer, Integer> cluster = new Cluster<>(new HashMap<>(), MatchPredicate.equals());

        Optional<Stream<Integer>> streamOptional = cluster.getCluster(1);

        assertThat(streamOptional.isPresent(), is(false));
    }

    @Test
    public void matchSingleClusterByEquality() throws Exception {
        HashMap<Integer, List<Integer>> map = MapBuilder
                .map(1, asList(1, 2, 3))
                .and(2, asList(4, 5, 6))
                .build();
        Cluster<Integer, Integer, Integer> cluster = new Cluster<>(map, MatchPredicate.equals());

        Stream<Integer> stream = cluster.getCluster(1).get();

        assertThat(stream, isStreamOf(1,2,3));
    }

    @Test
    public void matchesManyByPredicate() throws Exception {
        HashMap<Integer, List<Integer>> map = MapBuilder
                .map(1, asList(1, 2))
                .and(2, asList(3, 4))
                .and(3, asList(5, 6))
                .build();
        Cluster<Integer, Integer, Integer> cluster = new Cluster<>(map, (k1, k2) -> k1 <= k2);

        Stream<Integer> stream = cluster.getCluster(2).get();

        assertThat(stream, isStreamOf(3,4,5,6));
    }

    @Test
    public void matchNothingWithDisjointKeyTypesAndEquality() throws Exception {
        HashMap<String, List<Integer>> map = MapBuilder
                .map("1", asList(1, 2))
                .build();
        Cluster<Integer, String, Integer> cluster = new Cluster<>(map, MatchPredicate.equals());

        assertThat(cluster.getCluster(1).isPresent(), is(false));
    }

    @Test
    public void matchManyWithKeySubtypeAndPredicate() throws Exception {
        HashMap<String, List<Integer>> map = MapBuilder
                .map("1",  asList(1, 2))
                .and("2",  asList(3, 4))
                .and("3",  asList(5, 6))
                .build();
        Cluster<Integer, String, Integer> cluster = new Cluster<>(map, (k1, k2) -> k1 < Integer.valueOf(k2));

        Stream<Integer> stream = cluster.getCluster(1).get();

        assertThat(stream, isStreamOf(3, 4, 5, 6));
    }

    @Test
    public void emptyOptionalOfEmptyStream() throws Exception {
        Optional<Stream<Integer>> streamOptional = Cluster.emptyIfStreamIsEmpty(Stream.<Integer>empty());

        assertThat(streamOptional.isPresent(), is(false));
    }

    @Test
    public void nonemptyOptionalOfStream() throws Exception {
        Optional<Stream<Integer>> streamOptional = Cluster.emptyIfStreamIsEmpty(Stream.of(1, 2, 3));

        assertThat(streamOptional.get(), isStreamOf(1, 2, 3));
    }

    private static class MapBuilder<K, V> {
        private HashMap<K, V> map;
        public MapBuilder(HashMap<K, V> map) {
            this.map = map;
        }

        static <K, V> MapBuilder<K, V> map(K key, V value) {
            HashMap<K, V> map = new HashMap<>();
            map.put(key, value);
            return new MapBuilder<>(map);
        }

        public MapBuilder<K, V> and(K key, V value) {
            map.put(key, value);
            return this;
        }

        public HashMap<K, V> build() {
            return map;
        }
    }
}