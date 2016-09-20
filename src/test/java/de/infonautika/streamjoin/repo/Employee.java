package de.infonautika.streamjoin.repo;

import java.util.Objects;

public class Employee {
    private final Integer departmentId;
    private final String name;

    public Employee(Integer departmentId, String name) {
        this.departmentId = departmentId;
        this.name = name;
    }

    public Integer getDepartmentId() {
        return departmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return Objects.equals(departmentId, employee.departmentId) &&
                Objects.equals(name, employee.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(departmentId, name);
    }

    @Override
    public String toString() {
        return "Employee{" +
                departmentId +
                ", " + name +
                '}';
    }

    public String getName() {
        return name;
    }
}
