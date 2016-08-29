package de.infonautika.streamjoin.joins.repo;

import java.util.Objects;

public class Tuple<A, B> {

    private final A first;
    private final B second;

    public Tuple(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A, B> Tuple<A, B> tuple(A first, B second){
        return new Tuple<>(first, second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(first, tuple.first) &&
                Objects.equals(second, tuple.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
