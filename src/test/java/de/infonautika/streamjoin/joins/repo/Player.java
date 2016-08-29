package de.infonautika.streamjoin.joins.repo;

import java.util.Objects;

public class Player {
    private final String name;
    private final Integer rank;

    public Player(String name, Integer rank) {
        this.name = name;
        this.rank = rank;
    }

    public String getName() {
        return name;
    }

    public Integer getRank() {
        return rank;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Player player = (Player) o;
        return Objects.equals(name, player.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Player{" +
                "name='" + name + '\'' +
                ", rank=" + rank +
                '}';
    }
}
