package de.infonautika.streamjoin.repo;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class TestRepository {
    public static final Employee rafferty = new Employee(31, "Rafferty");
    public static final Employee jones = new Employee(33, "Jones");
    public static final Employee heisenberg = new Employee(33, "Heisenberg");
    public static final Employee robinson = new Employee(34, "Robinson");
    public static final Employee smith = new Employee(34, "Smith");
    public static final Employee williams = new Employee(null, "Williams");
    public static final Employee scruffy = new Employee(99, "Scruffy");

    public static final Department sales = new Department(31, "Sales");
    public static final Department salesTwo = new Department(31, "SalesTwo");
    public static final Department engineering = new Department(33, "Engineering");
    public static final Department clerical = new Department(34, "Clerical");
    public static final Department marketing = new Department(35, "Marketing");
    public static final Department storage = new Department(null, "storage");

    private static final List<Employee> employees = unmodifiableList(asList(
            rafferty,
            jones,
            heisenberg,
            robinson,
            smith,
            williams,
            scruffy));

    private static final List<Department> departments = unmodifiableList(asList(
            sales,
            salesTwo,
            engineering,
            clerical,
            marketing,
            storage));

    public static Stream<Employee> getEmployees() {
        return employees.stream();
    }

    public static Stream<Employee> employeesWithDepartment() {
        return getEmployees().filter(e -> e.getDepartmentId() != null);
    }

    public static Stream<Department> getDepartments() {
        return departments.stream();
    }

    public static Stream<Department> departmentsWithId() {
        return getDepartments().filter(d -> d.getId() != null);
    }
}
