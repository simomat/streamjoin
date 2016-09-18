package de.infonautika.streamjoin.repo;

import java.util.Objects;

public class Employee {
    private final Integer departmentId;
    private final String lastName;

    public Employee(Integer departmentId, String lastName) {
        this.departmentId = departmentId;
        this.lastName = lastName;
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
                Objects.equals(lastName, employee.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(departmentId, lastName);
    }

    @Override
    public String toString() {
        return "Employee{" +
                "departmentId=" + departmentId +
                ", lastName='" + lastName + '\'' +
                '}';
    }
}
