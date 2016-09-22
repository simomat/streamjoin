package de.infonautika.streamjoin.repo;

import java.util.Objects;

public class Department {
    public static final Department sentinel = new DepartmentSentinel();
    private final Integer id;
    private final String name;

    public String getName() {
        return name;
    }

    public Department(Integer id, String departmentName) {
        this.id = id;
        this.name = departmentName;
    }

    public Integer getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Department that = (Department) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "Department{" +
                id +
                ", " + name  +
                '}';
    }


    private static class DepartmentSentinel extends Department {
        public DepartmentSentinel() {
            super(999, "SENTINEL");
        }
    }
}
